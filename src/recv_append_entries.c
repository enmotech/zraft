#include "recv_append_entries.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "configuration.h"
#include "tracing.h"

static void recvSendAppendEntriesResultCb(struct raft_io_send *req, int status)
{
	(void)status;
	HeapFree(req);
}

int recvAppendEntries(struct raft *r,
		      raft_id id,
		      const char *address,
		      const struct raft_append_entries *args)
{
	struct raft_io_send *req;
	struct raft_message message;
	struct raft_append_entries_result *result = &message.append_entries_result;
	int match;
	bool async;
	int rv;

	struct pgrep_permit_info pi = args->pi;

	assert(r != NULL);
	assert(id > 0);
	assert(args != NULL);
	assert(address != NULL);

	tracef("[raft][%d][%d][pkt:%u][%s]: replicating[%d] permit[%d] "
		   "args->term[%lld] prev_log_index[%lld] entries[%d]",
	       rkey(r), r->state, args->pkt, __func__, args->pi.replicating,
	       args->pi.permit, args->term, args->prev_log_index, args->n_entries);

	result->rejected = args->prev_log_index;
	result->last_log_index = logLastIndex(&r->log);


	rv = recvEnsureMatchingTerms(r, args->term, &match);
	if (rv != 0) {
		tracef("[raft][%d][%d]recvEnsureMatchingTerms failed!",
			rkey(r), r->state);
		return rv;
	}

	/* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
	if (match < 0) {
		tracef("[raft][%d][%d]recvEnsureMatchingTerms local term is higher -> reject!",
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
		tracef("[raft][%d][%d]discovered leader -> step down!",
			  rkey(r), r->state);
		convertToFollower(r);
	}

	assert(r->state == RAFT_FOLLOWER);

	/* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
	rv = recvUpdateLeader(r, id, address);
	if (rv == RAFT_NOMEM) {
		tracef("[raft][%d][%d]recvUpdateLeader failed!",
			rkey(r), r->state);
		return rv;
	}

	/* Reset the election timer. */
	r->election_timer_start = r->io->time(r->io);

	if (args->pi.replicating) {
		struct raft_server *server = (struct raft_server *)configurationGet(&r->configuration, r->id);
		if (!r->pgrep_reported && server && r->role_change_cb) {
			server->role = RAFT_STANDBY;
			r->role_change_cb(r, server);
			r->pgrep_reported = true;
			tracef("[raft][%d][%d][%s][role_notify] role[%d].",
			       rkey(r), r->state, __func__, server->role);
		}
	} else {
		if (rv == 0 && r->state_change_cb)
			r->state_change_cb(r, RAFT_FOLLOWER);
		else
			rv = 0;
		r->pgrep_reported = false;
	}

	if (args->pi.replicating == PGREP_RND_HRT) {
		goto reply;
	}

	/* If we are installing a snapshot, ignore these entries. TODO: we should do
     * something smarter, e.g. buffering the entries in the I/O backend, which
     * should be in charge of serializing everything. */
	if (r->snapshot.put.data != NULL && args->n_entries > 0) {
		tracef("[raft][%d][%d]ignoring AppendEntries RPC during snapshot install!",
			  rkey(r), r->state);
		entryBatchesDestroy(args->entries, args->n_entries);
		return 0;
	}

	rv = replicationAppend(r, args, &result->rejected, &async, &pi);
	if (rv != 0) {
		tracef("[raft][%d][%d]replicationAppend failed rv[%d]!",
			rkey(r), r->state, rv);
		if (rv != RAFT_APPLY_BUSY &&
		    rv != RAFT_LOG_BUSY)
			return rv;
	}

	if (async) {
		return 0;
	}

	/* Echo back to the leader the point that we reached. */
	result->last_log_index = r->last_stored;

reply:
	result->term = r->current_term;
	result->pi = pi;
	result->pkt = args->pkt;

	/* Pgrep:
     *
	 *   If it's pgrep progress, should not reject the request.
     */
	if (args->pi.replicating) {
		result->rejected = 0;
		if (rv) {
			result->pi.replicating = PGREP_RND_ERR;
		}
	}

	/* Free the entries batch, if any. */
	if (args->n_entries > 0 && args->entries[0].batch != NULL) {
		raft_entry_batch_free(&args->entries[0]);
	}

	if (args->entries != NULL) {
		raft_free(args->entries);
	}

	message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
	message.server_id = id;
	message.server_address = address;
	req = HeapMalloc(sizeof *req);
	if (req == NULL) {
		return RAFT_NOMEM;
	}
	req->data = r;

	rv = r->io->send(r->io, req, &message, recvSendAppendEntriesResultCb);
	if (rv != 0) {
		raft_free(req);
		return rv;
	}

	return 0;
}

#undef tracef
