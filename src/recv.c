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
#include "event.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif
struct set_meta_req
{
    struct raft	*raft; /* Instance that has submitted the request */
    raft_term	term;
    raft_id		voted_for;
    struct raft_message message;
    struct raft_io_set_meta req;
};

int recvUpdateMeta(struct raft *r,
        struct raft_message *message,
        raft_term	term,
        raft_id voted_for,
        raft_io_set_meta_cb cb)
{
    int rv;
    struct set_meta_req *request;

    assert(term > r->current_term || r->voted_for != voted_for);
    tracef("remote term %lld is higher than %lld -> bump local term",
	     term, r->current_term);

    if (r->state != RAFT_FOLLOWER) {
        tracef("state %d step down", r->state);
    }

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("E-1528-170", "raft(%llx) malloc failed %d", r->id, rv);
        goto meta_err_nomen;
    }
    if(message) {
       request->message = *message;
    }
    request->raft = r;
    request->term = term;
    request->voted_for = voted_for;
    request->req.data = request;
    r->io->state = RAFT_IO_BUSY;
    evtNoticef("N-1528-038", "raft(%llx) recv set meta term %u vote_for %llx", r->id,
	       request->term, request->voted_for);
    rv = r->io->set_meta(r->io,
                 &request->req,
                 term,
                 voted_for,
                 cb);
    if (rv != 0) {
        evtErrf("E-1528-171", "raft(%llx) set meta failed %d", r->id, rv);
        goto meta_io_err;
    }
    return 0;
meta_io_err:
    raft_free(request);
meta_err_nomen:
    return rv;
}

static void freeRaftMessageData(struct raft_message *message)
{
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
}

static void recvBumpTermIOCb(struct raft_io_set_meta *req, int status)
{
    struct set_meta_req *request = req->data;
    struct raft *r = request->raft;

    if (r->state == RAFT_UNAVAILABLE) {
        evtErrf("E-1528-172", "raft(%llx) bump set meta cb, state is unavailable", r->id);
        goto err;
    }

    r->io->state = RAFT_IO_AVAILABLE;
    if (r->state != RAFT_FOLLOWER) {
        /* Also convert to follower. */
        convertToFollower(r);
    }

    if(status != 0) {
        evtErrf("E-1528-173", "raft %x bump term failed %d", r->id, status);
        goto err;
    }

    evtNoticef("N-1528-039", "raft(%llx) set meta succeed %u %llx", r->id, request->term,
        r->voted_for);
    r->current_term = request->term;
    r->voted_for = request->voted_for;

    recvCb(r->io, &request->message);
    raft_free(request);
    return;
err:
    freeRaftMessageData(&request->message);
    raft_free(request);
}

static int recvEnsureMatchingTerm(struct raft *r,
                  struct raft_message *message,
                  bool *async)
{
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
    assert(r);
    assert(message);
    assert(async);
    raft_term term;
    int match;
    /* check and asynchronously bump term */
    switch (message->type) {
    case RAFT_IO_APPEND_ENTRIES:
        term = message->append_entries.term;
        break;
    case RAFT_IO_APPEND_ENTRIES_RESULT:
        term = message->append_entries_result.term;
        break;
    case RAFT_IO_INSTALL_SNAPSHOT:
        term = message->install_snapshot.term;
        break;
    case RAFT_IO_TIMEOUT_NOW:
        term = message->timeout_now.term;
        break;
    case RAFT_IO_REQUEST_VOTE:
        if (message->request_vote.disrupt_leader) {
            term = message->request_vote.term;
            if (r->state == RAFT_LEADER && r->current_term < term) {
                convertToFollower(r);
            }
        }
        *async = false;
        return 0;
    case RAFT_IO_REQUEST_VOTE_RESULT:
        term = message->request_vote_result.term;
        break;
    default:
        *async = false;
        return 0;
    }
    recvCheckMatchingTerms(r, term, &match);
    if(match > 0) {
        *async = true;
        return recvUpdateMeta(r,
                              message,
                              term,
                              0,
                              recvBumpTermIOCb);
    }
    *async = false;
    return 0;
}
/* Dispatch a single RPC message to the appropriate handler. */
static int recvMessage(struct raft *r, struct raft_message *message)
{
    int rv = 0;
    bool async;

    if (message->type < RAFT_IO_APPEND_ENTRIES ||
        message->type > RAFT_IO_TIMEOUT_NOW) {
        tracef("received unknown message type type: %d", message->type);
        evtErrf("E-1528-174", "raft(%llx) received unknown message type %d",
		r->id, message->type);
        return 0;
    }

    rv = recvEnsureMatchingTerm(r, message, &async);
    if (rv != 0)
        evtErrf("E-1528-175", "raft(%llx) ensure matching term failed %d", r->id, rv);

    if (async) {
        return rv;
    }
    assert(r->io->state == RAFT_IO_AVAILABLE);

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = recvAppendEntries(r,
                                   message->server_id,
                                   &message->append_entries);
            if (rv != 0) {
                entryBatchesDestroy(message->append_entries.entries,
                                    message->append_entries.n_entries);
            }
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            rv = recvAppendEntriesResult(r, message->server_id,
                                         &message->append_entries_result);
            break;
        case RAFT_IO_REQUEST_VOTE:
            rv = recvRequestVote(r, message->server_id,
                                 &message->request_vote);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            rv = recvRequestVoteResult(r, message->server_id,
                                       &message->request_vote_result);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = recvInstallSnapshot(r, message->server_id,
                                     &message->install_snapshot);
            /* Already installing a snapshot, wait for it and ignore this one */
            if (rv == RAFT_BUSY) {
                raft_free(message->install_snapshot.data.base);
                raft_configuration_close(&message->install_snapshot.conf);
                rv = 0;
            }
            break;
        case RAFT_IO_TIMEOUT_NOW:
            rv = recvTimeoutNow(r,
                                message->server_id,
                                &message->timeout_now);
            break;
    };

    if (rv != 0 && rv != RAFT_NOCONNECTION) {
        evtErrf("E-1528-176", "raft(%llx) recv message %d failed %d",
		r->id, message->type, rv);
        return rv;
    }

    /* If there's a leadership transfer in progress, check if it has
     * completed. */
    if (r->transfer != NULL) {
        if (r->state == RAFT_FOLLOWER &&
                r->follower_state.current_leader.id == r->transfer->id) {
            membershipLeadershipTransferClose(r);
        }
    }

    return 0;
}

void recvCb(struct raft_io *io, struct raft_message *message)
{
    struct raft *r = io->data;
    int rv;
    if (r->state == RAFT_UNAVAILABLE ||
            r->io->state != RAFT_IO_AVAILABLE) {
        freeRaftMessageData(message);
        return;
    }
    rv = recvMessage(r, message);
    if (rv != 0) {
        evtErrf("E-1528-177", "raft %x recv msg %d from %x failed %d", r->id, message->type,
		    message->server_id, rv);
        convertToUnavailable(r);
    }
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
int recvUpdateLeader(struct raft *r, const raft_id id)
{
    assert(r->state == RAFT_FOLLOWER);

    if (r->follower_state.current_leader.id != id) {
        r->follower_aux.match_leader = false;
        r->follower_state.current_leader.id = id;
	    if (r->state_change_cb)
		    r->state_change_cb(r, RAFT_FOLLOWER);
    }

    return 0;
}
#undef tracef
