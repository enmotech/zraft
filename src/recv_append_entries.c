#include "recv_append_entries.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif


void sendAppendEntriesResult(
	struct raft *r,
	const struct raft_append_entries_result *result,
	const struct raft_append_entries *args);


int recvAppendEntries(struct raft *r,
                      raft_id id,
                      const char *address,
                      const struct raft_append_entries *args)
{
	struct raft_append_entries_result result;
    int match;
    bool async;
    int rv;

	struct pgrep_permit_info pi = args->pi;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);
    assert(address != NULL);

	ZSINFO(gzlog, "[raft][%d][%d][pkt:%d]recvAppendEntries: replicating[%d] permit[%d]",
		   rkey(r), r->state, args->pkt, args->pi.replicating, args->pi.permit);

    result.rejected = args->prev_log_index;
    result.last_log_index = logLastIndex(&r->log);

    rv = recvEnsureMatchingTerms(r, args->term, &match);
    if (rv != 0) {
		ZSERROR(gzlog, "[raft][%d][%d]recvEnsureMatchingTerms failed!",
				rkey(r), r->state);
        return rv;
    }

	if (args->term > r->current_term)
		r->last_append_time = 0;

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
    if (match < 0) {
		ZSWARNING(gzlog, "[raft][%d][%d]recvEnsureMatchingTerms local term is higher -> reject!",
				  rkey(r), r->state);
        goto reply;
    }

    /* If we get here it means that the term in the request matches our current
     * term or it was higher and we have possibly stepped down, because we
     * discovered the current leader:
     *
     * From Figure 3.1:
     *
     *   Rules for Servers: Candidates: if AppendEntries RPC is received from
     *   new leader: convert to follower.
     *
     * From Section 3.4:
     *
     *   While waiting for votes, a candidate may receive an AppendEntries RPC
     *   from another server claiming to be leader. If the leader's term
     *   (included in its RPC) is at least as large as the candidate's current
     *   term, then the candidate recognizes the leader as legitimate and
     *   returns to follower state. If the term in the RPC is smaller than the
     *   candidate's current term, then the candidate rejects the RPC and
     *   continues in candidate state.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: discovers current leader -> [follower]
     *
     * Note that it should not be possible for us to be in leader state, because
     * the leader that is sending us the request should have either a lower term
     * (and in that case we reject the request above), or a higher term (and in
     * that case we step down). It can't have the same term because at most one
     * leader can be elected at any given term.
     */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_CANDIDATE) {
        /* The current term and the peer one must match, otherwise we would have
         * either rejected the request or stepped down to followers. */
        assert(match == 0);
		ZSWARNING(gzlog, "[raft][%d][%d]discovered leader -> step down!",
				  rkey(r), r->state);
        convertToFollower(r);
    }

    assert(r->state == RAFT_FOLLOWER);

    /* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
    rv = recvUpdateLeader(r, id, address);
    if (rv != 0) {
		ZSERROR(gzlog, "[raft][%d][%d]recvUpdateLeader failed!",
				rkey(r), r->state);
        return rv;
    }

    /* Reset the election timer. */
    r->election_timer_start = r->io->time(r->io);

	if (args->pi.replicating == PGREP_RND_HRT) {
		goto reply;
	}

    /* If we are installing a snapshot, ignore these entries. TODO: we should do
     * something smarter, e.g. buffering the entries in the I/O backend, which
     * should be in charge of serializing everything. */
    if (r->snapshot.put.data != NULL && args->n_entries > 0) {
		ZSWARNING(gzlog, "[raft][%d][%d]ignoring AppendEntries RPC during snapshot install!",
				  rkey(r), r->state);
        entryBatchesDestroy(args->entries, args->n_entries);
		if (args->pi.replicating) {
			rv = -1;
			goto reply;
		}
		return 0;
    }

	rv = replicationAppend(r, args, &result.rejected, &async, &pi);
	if (rv != 0) {
		ZSERROR(gzlog, "[raft][%d][%d]replicationAppend failed rv[%d]!",
				rkey(r), r->state, rv);
		if (rv != RAFT_APPLY_BUSY &&
			rv != RAFT_LOG_BUSY)
			return rv;
	}

    if (async) {
        return 0;
    }

    /* Echo back to the leader the point that we reached. */
    result.last_log_index = r->last_stored;

reply:
    result.term = r->current_term;
	result.pi = pi;
	result.pkt = args->pkt;

	/* Pgrep:
     *
	 *   If it's pgrep progress, should not reject the request.
     */
	if (args->pi.replicating) {
		result.rejected = 0;
		if (rv) {
			result.pi.replicating = PGREP_RND_ERR;
		}
	}

    /* Free the entries batch, if any. */
    if (args->n_entries > 0 && args->entries[0].batch != NULL) {
        raft_free(args->entries[0].batch);
    }

    if (args->entries != NULL) {
        raft_free(args->entries);
    }

	sendAppendEntriesResult(r, &result, args);

    return 0;
}

#undef tracef
