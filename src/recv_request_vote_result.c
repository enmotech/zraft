#include "recv_request_vote_result.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"
#include "log.h"

#if defined(RAFT_ASYNC_ALL) && RAFT_ASYNC_ALL
int recvSetMeta(struct raft *r,
		struct raft_message *message,
		raft_term	term,
		raft_id voted_for,
		raft_io_set_meta_cb cb);
struct setMetar
{
	struct raft *raft; /* Instance that has submitted the request */
	raft_term	term;
	raft_id		voted_for;
	struct raft_message message;
	struct raft_io_set_meta req;
};

static void recvVoteResultBumpTermIOCb(struct raft_io_set_meta *req, int status)
{
	struct setMetar *request = req->data;
	struct raft *r = request->raft;

	if (r->state == RAFT_UNAVAILABLE)
		goto err;
	r->io->state = RAFT_IO_AVAILABLE;
	if(status != 0) {
		convertToUnavailable(r);
		goto err;
	}

	r->current_term = request->term;
	r->voted_for = request->voted_for;

	if (r->state != RAFT_FOLLOWER) {
		/* Also convert to follower. */
		convertToFollower(r);
	}
err:
	raft_free(request);
}


int recvRequestVoteResult(struct raft *r,
			   raft_id id,
			   const struct raft_request_vote_result *result)
{
	size_t votes_index;
	int match;
	int rv;

	assert(r != NULL);
	assert(id > 0);

	votes_index = configurationIndexOfVoter(&r->configuration, id);
	if (votes_index == r->configuration.n) {
		tracef("non-voting or unknown server -> reject");
		return 0;
	}

	/* Ignore responses if we are not candidate anymore */
	if (r->state != RAFT_CANDIDATE) {
		tracef("local server is not candidate -> ignore");
		return 0;
	}

	recvCheckMatchingTerms(r, result->term, &match);

	if (r->candidate_state.in_pre_vote) {
		if(match > 0) {
			if (result->term > (r->current_term + 1)) {
				assert(!result->vote_granted);
				return recvSetMeta(r,
						   NULL,
						   result->term,
						   0,
						   recvVoteResultBumpTermIOCb);
			}
		}
	} else {
		if(result->pre_vote) {
			//because the candidate did not persist the vote,
			tracef("the vote is pre-vote -> ignore");
			return 0;
		}

		if(match > 0) {
			assert(!result->vote_granted);
			return recvSetMeta(r,
					   NULL,
					   result->term,
					   0,
					   recvVoteResultBumpTermIOCb);
		} else if (match < 0) {
			/* If the term in the result is older than ours, this is an old message
			 * we should ignore, because the node who voted for us would have
			 * obtained our term.  This happens if the network is pretty choppy. */
			tracef("local term is higher -> ignore");
			return 0;
		}
	}

	if (result->vote_granted) {
		if (electionTally(r, votes_index)) {
			if (r->candidate_state.in_pre_vote) {
				tracef("votes quorum reached -> pre-vote successful");
				r->candidate_state.in_pre_vote = false;
				rv = electionStart(r);
				if (rv != 0) {
					return rv;
				}
			} else {
				tracef("votes quorum reached -> convert to leader");
				rv = convertToLeader(r);
				if (rv != 0) {
					return rv;
				}
				if (r->no_op) {
					/* add a no-op log */
					struct raft_buffer buf;

					buf.len = 8;
					buf.base = raft_entry_malloc(buf.len);
					if (buf.base == NULL)
						return RAFT_NOMEM;
					raft_index index = logLastIndex(&r->log) + 1;
					rv = logAppend(&r->log, r->current_term, RAFT_BARRIER, &buf, NULL, NULL);
					if (rv != 0) {
						raft_entry_free(buf.base);
						return  rv;
					}
					/* Send initial heartbeat. */
					rv = replicationTrigger(r, index);
					assert(rv == 0);
					return rv;
				} else {
					replicationHeartbeat(r);
				}
			}
		} else {
			tracef("votes quorum not reached");
		}
	} else {
		tracef("vote was not granted");
	}

	return 0;
}
#else
int recvRequestVoteResult(struct raft *r,
			  raft_id id,
			  const char *address,
			  const struct raft_request_vote_result *result)
{
	size_t votes_index;
	int match;
	int rv;

	(void)address;

	assert(r != NULL);
	assert(id > 0);

	votes_index = configurationIndexOfVoter(&r->configuration, id);
	if (votes_index == r->configuration.n) {
		tracef("non-voting or unknown server -> reject");
		return 0;
	}

	/* Ignore responses if we are not candidate anymore */
	if (r->state != RAFT_CANDIDATE) {
		tracef("local server is not candidate -> ignore");
		return 0;
	}

	/* If we're in the pre-vote phase, don't actually increment our term right
     * now (we'll do it later, if we start the second phase), and also don't
     * step down if the peer is just one term ahead (this is okay as in the
     * request we sent our current term plus one). */
	if (r->candidate_state.in_pre_vote) {
		recvCheckMatchingTerms(r, result->term, &match);
	} else {
		rv = recvEnsureMatchingTerms(r, result->term, &match);
		if (rv != 0) {
			return rv;
		}
	}

	if (match < 0) {
		/* If the term in the result is older than ours, this is an old message
	 * we should ignore, because the node who voted for us would have
	 * obtained our term.  This happens if the network is pretty choppy. */
		tracef("local term is higher -> ignore");
		return 0;
	}

	/* If we're in the pre-vote phase, check that the peer's is at most one term
     * ahead (possibly stepping down). If we're the actual voting phase, we
     * expect our term must to be the same as the response term (otherwise we
     * would have either ignored the result bumped our term). */
	if (r->candidate_state.in_pre_vote) {
		if (match > 0) {
			if (result->term > r->current_term + 1) {
				assert(!result->vote_granted);
				rv = recvBumpCurrentTerm(r, result->term);
				if (rv != 0) {
					return rv;
				}
			}
		}
	} else {
		assert(result->term == r->current_term);
	}

	/* If the vote was granted and we reached quorum, convert to leader.
     *
     * From Figure 3.1:
     *
     *   If votes received from majority of severs: become leader.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: receives votes from majority of servers -> [leader]
     *
     * From Section 3.4:
     *
     *   A candidate wins an election if it receives votes from a majority of
     *   the servers in the full cluster for the same term. Each server will
     *   vote for at most one candidate in a given term, on a
     *   firstcome-first-served basis [...]. Once a candidate wins an election,
     *   it becomes leader.
     */
	if (result->vote_granted) {
		if (electionTally(r, votes_index)) {
			if (r->candidate_state.in_pre_vote) {
				tracef("votes quorum reached -> pre-vote successful");
				r->candidate_state.in_pre_vote = false;
				rv = electionStart(r);
				if (rv != 0) {
					return rv;
				}
			} else {
				tracef("votes quorum reached -> convert to leader");
				rv = convertToLeader(r);
				if (rv != 0) {
					return rv;
				}
				/* Send initial heartbeat. */
				replicationHeartbeat(r);
			}
		} else {
			tracef("votes quorum not reached");
		}
	} else {
		tracef("vote was not granted");
	}

	return 0;
}
#endif
#undef tracef
