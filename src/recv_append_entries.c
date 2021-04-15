#include "recv_append_entries.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"
#if defined(RAFT_ASYNC_ALL) && RAFT_ASYNC_ALL
#include <string.h>
#endif
/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void recvSendAppendEntriesResultCb(struct raft_io_send *req, int status)
{
	(void)status;
	HeapFree(req);
}
#if defined(RAFT_ASYNC_ALL) && RAFT_ASYNC_ALL
/* Context for a write log entries request that was submitted by a follower. */
struct setMeta
{
    struct raft *raft; /* Instance that has submitted the request */
    raft_id	id;
    char	*address;
    struct raft_append_entries args;
    struct raft_io_set_meta req;
};

static void recvAECb(struct raft_io_set_meta *req, int status)
{
	struct setMeta *request = req->data;
	struct raft *r = request->raft;

	r->io->state = RAFT_IO_AVAILABLE;
	if(status != 0)
		goto err;

	r->current_term = request->args.term;
	r->voted_for = 0;

	if (r->state != RAFT_FOLLOWER) {
	    /* Also convert to follower. */
	    convertToFollower(r);
	}

	status = recvAppendEntries(r, request->id,
				   request->address,
				   &request->args);
	if(status != 0) {
err:
		entryBatchesDestroy(request->args.entries,
				    request->args.n_entries);
		convertToUnavailable(r);
	}
	raft_free(request->address);
	raft_free(request);
}
#endif

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

	assert(r != NULL);
	assert(id > 0);
	assert(args != NULL);
	assert(address != NULL);

	result->rejected = args->prev_log_index;
	result->last_log_index = logLastIndex(&r->log);

#if defined(RAFT_ASYNC_ALL) && RAFT_ASYNC_ALL
	recvCheckMatchingTerms(r, args->term, &match);
	if (match == 1) {
		struct setMeta *request;
		char msg[128];

		sprintf(msg, "remote term %lld is higher than %lld -> bump local term",
			args->term, r->current_term);
		if (r->state != RAFT_FOLLOWER) {
		    strcat(msg, " and step down");
		}


		request = raft_malloc(sizeof *request);
		if (request == NULL) {
			rv = RAFT_NOMEM;
			goto err2;
		}

		request->address = raft_malloc(strlen(address) + 1);
		if (request->address == NULL) {
			rv = RAFT_NOMEM;
			goto err1;
		}

		request->raft = r;
		strcpy(request->address, address);
		request->args = *args;
		request->req.data = request;

		rv = r->io->set_meta(r->io,
				&request->req,
				args->term,
				id,
				recvAECb);
		if (rv != 0)
			goto err;

		r->io->state = RAFT_IO_BUSY;

		return 0;
		err:
		raft_free(request->address);
		err1:
		raft_free(request);
		err2:
		return rv;
	}
#else
	rv = recvEnsureMatchingTerms(r, args->term, &match);
	if (rv != 0) {
		return rv;
	}
#endif


	/* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
	if (match < 0) {
		tracef("local term is higher -> reject ");
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
		tracef("discovered leader -> step down ");
		convertToFollower(r);
	}

	assert(r->state == RAFT_FOLLOWER);

	/* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
	rv = recvUpdateLeader(r, id, address);
	if (rv != 0) {
		return rv;
	}

	/* Reset the election timer. */
	r->election_timer_start = r->io->time(r->io);

	/* If we are installing a snapshot, ignore these entries. TODO: we should do
     * something smarter, e.g. buffering the entries in the I/O backend, which
     * should be in charge of serializing everything. */
	if (r->snapshot.put.data != NULL && args->n_entries > 0) {
		tracef("ignoring AppendEntries RPC during snapshot install");
		entryBatchesDestroy(args->entries, args->n_entries);
		return 0;
	}

	rv = replicationAppend(r, args, &result->rejected, &async);
	if (rv != 0) {
		return rv;
	}

	if (async) {
		return 0;
	}

	/* Echo back to the leader the point that we reached. */
	result->last_log_index = r->last_stored;

reply:
	result->term = r->current_term;

	/* Free the entries batch, if any. */
	if (args->n_entries > 0 && args->entries[0].batch != NULL) {
		raft_free(args->entries[0].batch);
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
