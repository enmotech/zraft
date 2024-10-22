#include "recv_append_entries.h"

#include "assert.h"
#include "convert.h"
#include "entry.h"
#include "heap.h"
#include "log.h"
#include "recv.h"
#include "replication.h"
#include "tracing.h"
#include "event.h"
#include "hook.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void recvSendAppendEntriesResultCb(struct raft_io_send *req, int status)
{
    (void)status;
    HeapFree(req);
}

static void recvInvokeEntryHook(struct raft *r,
				const struct raft_append_entries *args,
				raft_index rejected)
{
	if (r->follower_aux.match_leader && rejected == 0)
		return;
	if (!r->follower_aux.match_leader && rejected != 0)
		return;

	r->follower_aux.match_leader = rejected == 0;
	r->hook->entry_match_change_cb(r->hook, rejected == 0,
				       args->prev_log_index,
				       args->prev_log_term);
}

static void recvUpdateLeaderSnapshot(struct raft *r,
					  raft_index snapshot_index, unsigned trailing)
{
	assert(r->state == RAFT_FOLLOWER);
	if (r->follower_state.current_leader.snapshot_index == snapshot_index &&
	    r->follower_state.current_leader.trailing == trailing)
		return;

	evtInfof("I-1528-002",
		   "raft(%llx) update leader snapshot index %llu trailing %u",
		   r->id, snapshot_index, trailing);
	r->follower_state.current_leader.snapshot_index = snapshot_index;
    r->follower_state.current_leader.trailing = trailing;
}

int recvAppendEntries(struct raft *r,
                      raft_id id,
                      const struct raft_append_entries *args)
{
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int match;
    bool async;
    int rv;
    bool discard;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);
    if (r->prev_append_status) {
        evtErrf("E-1528-159", "raft(%llx) reject append with prev append status %d",
		r->id, r->prev_append_status);
        return RAFT_NOCONNECTION;
    }
    result->pkt = args->pkt;
    result->rejected = args->prev_log_index;
    result->last_log_index = logLastIndex(&r->log);
    result->n_entries = args->n_entries;
    result->timestamp = args->timestamp;

    recvCheckMatchingTerms(r, args->term, &match);
    assert(match <= 0);
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
    rv = recvUpdateLeader(r, id);
    if (rv != 0) {
        evtErrf("E-1528-160", "raft(%llx) update leader failed %d", r->id, rv);
        return rv;
    }

    /* Reset the election timer. */
    r->election_timer_start = r->io->time(r->io);

    if (hookHackAppendEntries(r, args, result, &discard)) {
        if (discard)
            goto err_free_args;
        goto reply;
    }

    /* If we are installing a snapshot, ignore these entries. TODO: we should do
     * something smarter, e.g. buffering the entries in the I/O backend, which
     * should be in charge of serializing everything. */
    if (replicationInstallSnapshotBusy(r) && args->n_entries > 0) {
        tracef("ignoring AppendEntries RPC during snapshot install");
        entryBatchesDestroy(args->entries, args->n_entries);
        return 0;
    }

    rv = replicationAppend(r, args, &result->rejected, &async,
                           &result->last_log_index);
    if (rv != 0) {
        evtErrf("E-1528-161", "raft(%llx) replication append %d", r->id, rv);
        return rv;
    }

    if (!result->rejected)
	    recvUpdateLeaderSnapshot(r, args->snapshot_index, args->trailing);

    recvInvokeEntryHook(r, args, result->rejected);
    if (async) {
        return 0;
    }
reply:
    result->term = r->current_term;
    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;

    req = HeapMalloc(sizeof *req);
    if (req == NULL) {
        evtErrf("E-1528-162", "%s", "malloc");
        return RAFT_NOMEM;
    }
    req->data = r;

    tracef("send %llu aer term %llu reject %llu last_index %llu", id,
        result->term, result->rejected, result->last_log_index);

    rv = r->io->send(r->io, req, &message, recvSendAppendEntriesResultCb);
    if (rv != 0) {
        if (rv != RAFT_NOCONNECTION)
            evtErrf("E-1528-163", "raft(%llx) send failed %d", r->id, rv);
        raft_free(req);
        return rv;
    }

err_free_args:
    entryBatchesDestroy(args->entries, args->n_entries);
    return 0;
}

#undef tracef
