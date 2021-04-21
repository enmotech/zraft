#include "recv.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "recv_append_entries.h"
#include "recv_append_entries_result.h"
#include "recv_install_snapshot.h"
#include "recv_request_vote.h"
#include "recv_request_vote_result.h"
#include "recv_timeout_now.h"
#include "string.h"
#include "tracing.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

#if defined(RAFT_ASYNC_ALL) && RAFT_ASYNC_ALL
struct setMetar
{
	struct raft *raft; /* Instance that has submitted the request */
	raft_term	term;
	raft_id		voted_for;
	struct raft_message message;
	struct raft_io_set_meta req;
};

static void recvBumpTermIOCb(struct raft_io_set_meta *req, int status)
{
	struct setMetar *request = req->data;
	struct raft *r = request->raft;
	char *address = (char *)(request->message.server_address);

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

	recvCb(r->io, &request->message);
err:
	raft_free(address);
	raft_free(request);
}

int recvSetMeta(struct raft *r,
		struct raft_message *message,
		raft_term	term,
		raft_id voted_for,
		raft_io_set_meta_cb cb)
{
	int rv;
	struct setMetar *request;
	char msg[128];
	char *address = NULL;

	assert(term > r->current_term ||
			r->voted_for != voted_for);

	sprintf(msg, "remote term %lld is higher than %lld -> bump local term",
		term, r->current_term);

	if (r->state != RAFT_FOLLOWER) {
		strcat(msg, " and step down");
	}

	request = raft_malloc(sizeof *request);
	if (request == NULL) {
		rv = RAFT_NOMEM;
		goto err2;
	}

	request->raft = r;
	request->term = term;
	request->voted_for = voted_for;
	request->req.data = request;

	if(message) {
		address = raft_malloc(strlen(message->server_address) + 1);
		if (address == NULL) {
			rv = RAFT_NOMEM;
			goto err1;
		}
		request->message = *message;
		strcpy(address, message->server_address);
		request->message.server_address = address;
	}

	rv = r->io->set_meta(r->io,
			     &request->req,
			     term,
			     voted_for,
			     cb);
	if (rv != 0)
		goto err;

	r->io->state = RAFT_IO_BUSY;

	return 0;

err:
	if(address)
		raft_free(address);
err1:
	raft_free(request);
err2:
	return rv;
}
#endif

/* Dispatch a single RPC message to the appropriate handler. */
static int recvMessage(struct raft *r, struct raft_message *message)
{
	int rv = 0;

	int match;
	raft_term term = 0;

	if (message->type < RAFT_IO_APPEND_ENTRIES ||
	    message->type > RAFT_IO_TIMEOUT_NOW) {
		tracef("received unknown message type type: %d", message->type);
		return 0;
	}

#if defined(RAFT_ASYNC_ALL) && RAFT_ASYNC_ALL
	recvCheckMatchingTerms(r, term, &match);
	if(match > 0) {
		switch(message->type) {
		case RAFT_IO_APPEND_ENTRIES:
			return recvSetMeta(r,
					 message,
					 message->append_entries.term,
					 message->server_id,
					 recvBumpTermIOCb);
			break;
		case RAFT_IO_APPEND_ENTRIES_RESULT:
			return recvSetMeta(r,
					 message,
					 message->append_entries_result.term,
					 0,
					 recvBumpTermIOCb);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			return recvSetMeta(r,
					   message,
					   message->install_snapshot.term,
					   message->server_id,
					   recvBumpTermIOCb);
			break;
		case RAFT_IO_TIMEOUT_NOW:
			return recvSetMeta(r,
					   message,
					   message->timeout_now.term,
					   message->server_id,
					   recvBumpTermIOCb);
			break;
			default:
			break;
		}
	}
#endif
	/* tracef("%s from server %ld", message_descs[message->type - 1],
       message->server_id); */

	switch (message->type) {
	case RAFT_IO_APPEND_ENTRIES:
		rv = recvAppendEntries(r, message->server_id,
				       message->server_address,
				       &message->append_entries);
		if (rv != 0) {
			entryBatchesDestroy(message->append_entries.entries,
					    message->append_entries.n_entries);
		}
		break;
	case RAFT_IO_APPEND_ENTRIES_RESULT:
		rv = recvAppendEntriesResult(r, message->server_id,
					     message->server_address,
					     &message->append_entries_result);
		break;
	case RAFT_IO_REQUEST_VOTE:
		rv = recvRequestVote(r, message->server_id, message->server_address,
				     &message->request_vote);
		break;
	case RAFT_IO_REQUEST_VOTE_RESULT:
		rv = recvRequestVoteResult(r, message->server_id,
					   message->server_address,
					   &message->request_vote_result);
		break;
	case RAFT_IO_INSTALL_SNAPSHOT:
		rv = recvInstallSnapshot(r, message->server_id,
					 message->server_address,
					 &message->install_snapshot);
		/* Already installing a snapshot, wait for it and ignore this one */
		if (rv == RAFT_BUSY) {
			raft_free(message->install_snapshot.data.base);
			raft_configuration_close(&message->install_snapshot.conf);
			rv = 0;
		}
		break;
	case RAFT_IO_TIMEOUT_NOW:
		rv = recvTimeoutNow(r, message->server_id, message->server_address,
				    &message->timeout_now);
		break;
	};

	if (rv != 0 && rv != RAFT_NOCONNECTION) {
		/* tracef("recv: %s: %s", message_descs[message->type - 1],
		 raft_strerror(rv)); */
		return rv;
	}

	/* If there's a leadership transfer in progress, check if it has
     * completed. */
	if (r->io->state == RAFT_IO_AVAILABLE &&
	    r->transfer != NULL) {
		if (r->follower_state.current_leader.id == r->transfer->id) {
			membershipLeadershipTransferClose(r);
		}
	}

	return 0;
}

void recvCb(struct raft_io *io, struct raft_message *message)
{
	struct raft *r = io->data;
	int rv;
	if (r->state == RAFT_UNAVAILABLE) {
		switch (message->type) {
		case RAFT_IO_APPEND_ENTRIES:
			entryBatchesDestroy(message->append_entries.entries,
					    message->append_entries.n_entries);
			break;
		case RAFT_IO_INSTALL_SNAPSHOT:
			raft_configuration_close(&message->install_snapshot.conf);
			raft_free(message->install_snapshot.data.base);
			break;
		}
		return;
	}
	rv = recvMessage(r, message);
	if (rv != 0) {
		convertToUnavailable(r);
	}
}

int recvBumpCurrentTerm(struct raft *r, raft_term term)
{
	int rv;
	char msg[128];

	assert(r != NULL);
	assert(term > r->current_term);

	sprintf(msg, "remote term %lld is higher than %lld -> bump local term",
		term, r->current_term);
	if (r->state != RAFT_FOLLOWER) {
		strcat(msg, " and step down");
	}
	tracef("%s", msg);

	/* Save the new term to persistent store, resetting the vote. */
	rv = r->io->set_term(r->io, term);
	if (rv != 0) {
		return rv;
	}

	/* Update our cache too. */
	r->current_term = term;
	r->voted_for = 0;

	if (r->state != RAFT_FOLLOWER) {
		/* Also convert to follower. */
		convertToFollower(r);
	}

	return 0;
}

void recvCheckMatchingTerms(struct raft *r, raft_term term, int *match)
{
	if (term < r->current_term) {
		*match = -1;
	} else if (term > r->current_term) {
		*match = 1;
	} else {
		*match = 0;
	}
}

int recvEnsureMatchingTerms(struct raft *r, raft_term term, int *match)
{
	int rv;

	assert(r != NULL);
	assert(match != NULL);

	recvCheckMatchingTerms(r, term, match);

	if (*match == -1) {
		return 0;
	}

	/* From Figure 3.1:
     *
     *   Rules for Servers: All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     *
     * From state diagram in Figure 3.3:
     *
     *   [leader]: discovers server with higher term -> [follower]
     *
     * From Section 3.3:
     *
     *   If a candidate or leader discovers that its term is out of date, it
     *   immediately reverts to follower state.
     */
	if (*match == 1) {
		rv = recvBumpCurrentTerm(r, term);
		if (rv != 0) {
			return rv;
		}
	}

	return 0;
}

int recvUpdateLeader(struct raft *r, const raft_id id, const char *address)
{
	assert(r->state == RAFT_FOLLOWER);

	r->follower_state.current_leader.id = id;

	/* If the address of the current leader is the same as the given one, we're
     * done. */
	if (r->follower_state.current_leader.address != NULL &&
	    strcmp(address, r->follower_state.current_leader.address) == 0) {
		return 0;
	}

	if (r->follower_state.current_leader.address != NULL) {
		HeapFree(r->follower_state.current_leader.address);
	}
	r->follower_state.current_leader.address = HeapMalloc(strlen(address) + 1);
	if (r->follower_state.current_leader.address == NULL) {
		return RAFT_NOMEM;
	}
	strcpy(r->follower_state.current_leader.address, address);

	return 0;
}

#undef tracef
