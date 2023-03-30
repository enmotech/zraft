#include <string.h>
#include <stdlib.h>
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#ifdef __GLIBC__
#include "error.h"
#endif
#include "err.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "snapshot.h"
#include "tracing.h"
#include "event.h"
#include "hook.h"
#include "../test/lib/fsm.h"
#include "byte.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Callback invoked after request to send an AppendEntries RPC has completed. */
static void sendAppendEntriesCb(struct raft_io_send *send, const int status)
{
    struct sendAppendEntries *req = send->data;
    struct raft *r = req->raft;
    unsigned i = configurationIndexOf(&r->configuration, req->server_id);
    unsigned j;

    if (r->state == RAFT_LEADER && i < r->configuration.n) {
        if (status != 0) {
            tracef("failed to send append entries to server %llu: %s",
                   req->server_id, raft_strerror(status));
            evtErrf("raft(%llx) failed to send append entries to %llu %d",
		    r->id, req->server_id, status);
            /* Go back to probe mode. */
            progressToProbe(r, i);
        }
    }
    //对于在zstorage和raft-test里加载的entry.buf，需要在raft内部的sendAppendEntriesCb里释放
    if (req->n > 0) {
        for(j = 0; j < req->n; j++){
            if(req->entriesLoadByDisk[j] == true && req->entries[j].buf.base != NULL){
                raft_entry_free(req->entries[j].buf.base);
            }
        }
    }
    /* Tell the log that we're done referencing these entries. */
    logRelease(&r->log, req->index, req->entries, req->n);
    if (req->n > 0)
        raft_free(req->entriesLoadByDisk);
    raft_free(req);
}

/* Send an AppendEntries message to the i'th server, including all log entries
 * from the given point onwards. */
static int sendAppendEntries(struct raft *r,
                             const unsigned i,
                             const raft_index prev_index,
                             const raft_term prev_term)
{
    struct raft_server *server = &r->configuration.servers[i];
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    struct sendAppendEntries *req;
    raft_index next_index = prev_index + 1;
    raft_index optimistic_next_index;
    int rv;
    unsigned j;

    args->term = r->current_term;
    args->prev_log_index = prev_index;
    args->prev_log_term = prev_term;
    args->snapshot_index = r->log.snapshot.last_index;
    args->entries_reload = false;
    args->trailing = r->snapshot.trailing;

    rv = logAcquireWithMax(&r->log, next_index, &args->entries,
			   &args->n_entries, r->message_log_threshold);
    for(j = 0; j < args->n_entries; j++) {
        if(args->entries[j].buf.base == NULL){
            args->entries_reload = true;
        }
    }
    if (rv != 0) {
        evtErrf("raft(%llx) log acquire failed %d", r->id, rv);
        goto err;
    }

    /* From Section 3.5:
     *
     *   The leader keeps track of the highest index it knows to be committed,
     *   and it includes that index in future AppendEntries RPCs (including
     *   heartbeats) so that the other servers eventually find out. Once a
     *   follower learns that a log entry is committed, it applies the entry to
     *   its local state machine (in log order)
     */
    args->leader_commit = r->commit_index;

    tracef("send %u entries starting at %llu to server %llu (last index %llu)",
           args->n_entries, args->prev_log_index, server->id,
           logLastIndex(&r->log));

    if (prev_index == 0) {
	    evtWarnf("raft(%llx) send %llx entries %u  %llu %llu %llu",
		     r->id, server->id, args->n_entries, args->prev_log_index,
		     args->prev_log_term, logLastIndex(&r->log));
    }

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = server->id;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("%s", "malloc");
        goto err_after_entries_acquired;
    }
    req->raft = r;
    req->index = args->prev_log_index + 1;
    req->entries = args->entries;
    req->n = args->n_entries;
    req->server_id = server->id;
    if(req->n > 0) 
        req->entriesLoadByDisk = raft_calloc((size_t)req->n, sizeof(bool));
    req->send.data = req;
    optimistic_next_index = req->index + req->n;
    for (j = 0; j < req->n; j++) {
        if (req->entries[j].buf.len == 0 && req->entries[j].type != RAFT_BARRIER) {
            req->entriesLoadByDisk[j] = true;
        } else{
            req->entriesLoadByDisk[j] = false;
        }
    }
    rv = r->io->send(r->io, &req->send, &message, sendAppendEntriesCb);
    if (rv != 0) {
        if (rv != RAFT_NOCONNECTION)
            evtErrf("raft(%llx) send failed %d", r->id, rv);
        goto err_after_req_alloc;
    }

    if (progressState(r, i) == PROGRESS__PIPELINE) {
        /* Optimistically update progress. */
        progressOptimisticNextIndex(r, i, optimistic_next_index);
    }

    progressUpdateLastSend(r, i);
    return 0;

err_after_req_alloc:
    raft_free(req);
err_after_entries_acquired:
    logRelease(&r->log, next_index, args->entries, args->n_entries);
err:
    assert(rv != 0);
    return rv;
}

/* Context of a RAFT_IO_INSTALL_SNAPSHOT request that was submitted with
 * raft_io_>send(). */
struct sendInstallSnapshot
{
    struct raft *raft;               /* Instance sending the snapshot. */
    struct raft_io_snapshot_get get; /* Snapshot get request. */
    struct raft_io_send send;        /* Underlying I/O send request. */
    struct raft_snapshot *snapshot;  /* Snapshot to send. */
    raft_id server_id;               /* Destination server. */
};

static void sendInstallSnapshotCb(struct raft_io_send *send, int status)
{
    struct sendInstallSnapshot *req = send->data;
    struct raft *r = req->raft;
    const struct raft_server *server;

    server = configurationGet(&r->configuration, req->server_id);

    if (status != 0) {
        tracef("send install snapshot: %s", raft_strerror(status));
        if (r->state == RAFT_LEADER && server != NULL) {
            unsigned i;
            i = configurationIndexOf(&r->configuration, req->server_id);
            progressAbortSnapshot(r, i);
        }
    }

    snapshotClose(req->snapshot);
    raft_free(req->snapshot);
    raft_free(req);
}

static void sendSnapshotGetCb(struct raft_io_snapshot_get *get,
                              struct raft_snapshot *snapshot,
                              int status)
{
    struct sendInstallSnapshot *req = get->data;
    struct raft *r = req->raft;
    struct raft_message message;
    struct raft_install_snapshot *args = &message.install_snapshot;
    const struct raft_server *server = NULL;
    bool progress_state_is_snapshot = false;
    unsigned i = 0;
    int rv;

    if (status != 0) {
        tracef("get snapshot %s", raft_strerror(status));
        goto abort;
    }
    if (r->state != RAFT_LEADER) {
        goto abort_with_snapshot;
    }

    server = configurationGet(&r->configuration, req->server_id);

    if (server == NULL) {
        /* Probably the server was removed in the meantime. */
        goto abort_with_snapshot;
    }

    i = configurationIndexOf(&r->configuration, req->server_id);
    progress_state_is_snapshot = progressState(r, i) == PROGRESS__SNAPSHOT;

    if (!progress_state_is_snapshot) {
        /* Something happened in the meantime. */
        goto abort_with_snapshot;
    }

    assert(snapshot->n_bufs == 1);

    message.type = RAFT_IO_INSTALL_SNAPSHOT;
    message.server_id = server->id;

    args->term = r->current_term;
    args->last_index = snapshot->index;
    args->last_term = snapshot->term;
    args->conf_index = snapshot->configuration_index;
    args->conf = snapshot->configuration;
    args->data = snapshot->bufs[0];

    req->snapshot = snapshot;
    req->send.data = req;

    tracef("sending snapshot with last index %llu to %llu", snapshot->index,
           server->id);

    rv = r->io->send(r->io, &req->send, &message, sendInstallSnapshotCb);
    if (rv != 0) {
        if (rv != RAFT_NOCONNECTION)
            evtErrf("raft(%llx) send failed %d", r->id, rv);
        goto abort_with_snapshot;
    }

    goto out;

abort_with_snapshot:
    snapshotClose(snapshot);
    raft_free(snapshot);
abort:
    if (r->state == RAFT_LEADER && server != NULL &&
        progress_state_is_snapshot) {
        progressAbortSnapshot(r, i);
    }
    raft_free(req);
out:
    return;
}

/* Send the latest snapshot to the i'th server */
static int sendSnapshot(struct raft *r, const unsigned i)
{
    struct raft_server *server = &r->configuration.servers[i];
    struct sendInstallSnapshot *request;
    int rv;

    progressToSnapshot(r, i);

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("%s", "malloc");
        goto err;
    }
    request->raft = r;
    request->server_id = server->id;
    request->get.data = request;

    /* TODO: make sure that the I/O implementation really returns the latest
     * snapshot *at this time* and not any snapshot that might be stored at a
     * later point. Otherwise the progress snapshot_index would be wrong. */
    rv = r->io->snapshot_get(r->io, &request->get, sendSnapshotGetCb);
    if (rv != 0) {
        evtErrf("raft(%llx) snapshot get failed %d", r->id, rv);
        goto err_after_req_alloc;
    }

    if (r->state != RAFT_LEADER) {
        evtNoticef("raft(%llx) leader has step down after get snapshot", r->id);
        return 0;
    }
    progressUpdateSnapshotLastSend(r, i);
    return 0;

err_after_req_alloc:
    raft_free(request);
err:
    progressAbortSnapshot(r, i);
    assert(rv != 0);
    return rv;
}

int replicationProgress(struct raft *r, unsigned i)
{
    struct raft_server *server = &r->configuration.servers[i];
    bool progress_state_is_snapshot = progressState(r, i) == PROGRESS__SNAPSHOT;
    raft_index snapshot_index = logSnapshotIndex(&r->log);
    raft_index next_index = progressNextIndex(r, i);
    raft_index match_index = progressMatchIndex(r, i);
    raft_index match_term;
    raft_index prev_index;
    raft_term prev_term;

    assert(r->state == RAFT_LEADER);
    assert(server->id != r->id);
    assert(next_index >= 1);

    if (!progressShouldReplicate(r, i)) {
        return 0;
    }

    /* From Section 3.5:
     *
     *   When sending an AppendEntries RPC, the leader includes the index and
     *   term of the entry in its log that immediately precedes the new
     *   entries. If the follower does not find an entry in its log with the
     *   same index and term, then it refuses the new entries. The consistency
     *   check acts as an induction step: the initial empty state of the logs
     *   satisfies the Log Matching Property, and the consistency check
     *   preserves the Log Matching Property whenever logs are extended. As a
     *   result, whenever AppendEntries returns successfully, the leader knows
     *   that the follower's log is identical to its own log up through the new
     *   entries (Log Matching Property in Figure 3.2).
     */
    if (next_index == 1) {
        /* We're including the very first entry, so prevIndex and prevTerm are
         * null. If the first entry is not available anymore, send the last
         * snapshot if we're not already sending one. */
        if (snapshot_index > 0 && !progress_state_is_snapshot) {
            raft_index last_index = logLastIndex(&r->log);
            assert(last_index > 0); /* The log can't be empty */
	    evtNoticef("raft(%llx) send %llx snapshot %llu/%llu/%llu recent %d",
		       r->id, server->id, next_index,
		       r->leader_state.min_match_index,
		       snapshot_index,
		       progressGetRecentRecv(r, i));
            goto send_snapshot;
        }
        prev_index = 0;
        prev_term = 0;
    } else {
        /* Set prevIndex and prevTerm to the index and term of the entry at
         * next_index - 1. */
        prev_index = next_index - 1;
        prev_term = logTermOf(&r->log, prev_index);
        /* If the entry is not anymore in our log, send the last snapshot if we're
         * not doing so already. */
        if ((prev_term == 0 && !progress_state_is_snapshot)
        || (prev_index < snapshot_index && progressGetRecentRecv(r, i) == false)) {
            assert(prev_index < snapshot_index);
            tracef("missing entry at index %lld -> send snapshot", prev_index);
	    evtNoticef(
	    "raft(%llx) send %llx snapshot %llu/%llu/%llu/%llu recent %d",
		       r->id, server->id, next_index,
		       r->leader_state.min_match_index,
		       snapshot_index,
		       logLastIndex(&r->log),
		       progressGetRecentRecv(r, i));
            goto send_snapshot;
        }
    }

    /* Send empty AppendEntries RPC when installing a snaphot */
    if (progress_state_is_snapshot) {
        prev_index = logLastIndex(&r->log);
        prev_term = logLastTerm(&r->log);
    }

    return sendAppendEntries(r, i, prev_index, prev_term);

send_snapshot:
    if (progressGetRecentRecv(r, i)) {
        if(r->log.offset <= match_index && r->configuration.servers[i].id !=  r->leader_state.promotee_id) {
            match_term = match_index == 0 ? 0 : logTermOf(&r->log, match_index);
            return sendAppendEntries(r, i, match_index, match_term);
        }
        /* Only send a snapshot when we have heard from the server */
        return sendSnapshot(r, i);
    } else {
        /* Send empty AppendEntries RPC when we haven't heard from the server */
        prev_index = logLastIndex(&r->log);
        prev_term = logLastTerm(&r->log);
        return sendAppendEntries(r, i, prev_index, prev_term);
    }
}

/* Possibly trigger I/O requests for newly appended log entries or heartbeats.
 *
 * This function loops through all followers and triggers replication on them.
 *
 * It must be called only by leaders. */
static int triggerAll(struct raft *r)
{
    unsigned i;
    int rv;

    assert(r->state == RAFT_LEADER);

    /* Trigger replication for servers we didn't hear from recently. */
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id) {
            continue;
        }
        /* Skip spare servers, unless they're being promoted. */
        if (server->role == RAFT_SPARE &&
            server->id != r->leader_state.promotee_id) {
            continue;
        }
        rv = replicationProgress(r, i);
        if (rv != 0 && rv != RAFT_NOCONNECTION) {
            /* This is not a critical failure, let's just log it. */
            tracef("failed to send append entries to server %llu: %s (%d)",
                   server->id, raft_strerror(rv), rv);
            evtErrf("raft(%llx) send append entries to %llx failed %d",
		    r->id, server->id, rv);
        }
    }

    return 0;
}

int replicationHeartbeat(struct raft *r)
{
    return triggerAll(r);
}

/* Context for a write log entries request that was submitted by a leader. */
struct appendLeader
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
    struct raft_io_append req;
};

/* Called after a successful append entries I/O request to update the index of
 * the last entry stored on disk. Return how many new entries that are still
 * present in our in-memory log were stored. */
static size_t updateLastStored(struct raft *r,
                               raft_index first_index,
                               struct raft_entry *entries,
                               size_t n_entries)
{
    size_t i;

    /* Check which of these entries is still in our in-memory log */
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];
        raft_index index = first_index + i;
        raft_term local_term = logTermOf(&r->log, index);

        /* If we have no entry at this index, or if the entry we have now has a
         * different term, it means that this entry got truncated, so let's stop
         * here. */
        if (local_term == 0 || (local_term > 0 && local_term != entry->term)) {
            break;
        }

        /* If we do have an entry at this index, its term must match the one of
         * the entry we wrote on disk. */
        assert(local_term != 0 && local_term == entry->term);
    }

    r->last_stored += i;
    return i;
}

/* Get the request matching the given index, if any. */
static struct request *getRequest(struct raft *r,
                                  const raft_index index)
{
    if (r->state != RAFT_LEADER) {
        return NULL;
    }

    return requestRegDel(&r->leader_state.reg, index);
}

/* Invoked once a disk write request for new entries has been completed. */
static void appendLeaderCb(struct raft_io_append *req, int status)
{
    struct appendLeader *request = req->data;
    struct raft *r = request->raft;
    size_t server_index;
    int rv;

    tracef("leader: written %u entries starting at %lld: status %d", request->n,
           request->index, status);

    hookRequestAppendDone(r, request->index);
    assert(r->nr_appending_requests > 0);
    r->nr_appending_requests -= 1;
    /* In case of a failed disk write, if we were the leader creating these
     * entries in the first place, truncate our log too (since we have appended
     * these entries to it) and fire the request callback. */
    if (status != 0) {
        evtErrf("raft(%llx) append leader failed %d", r->id, status);
        struct raft_apply *apply;
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        apply =
            (struct raft_apply *)getRequest(r, request->index);
        if (apply != NULL) {
            if (apply->cb != NULL) {
                apply->cb(apply, status, NULL);
            }
        }

	if (r->state == RAFT_LEADER) {
		convertToFollower(r);
		evtNoticef("raft(%llx) convert from leader to follower",
			   r->id, status);
	}
        goto out;
    }


    updateLastStored(r, request->index, request->entries, request->n);

    /* If we are not leader anymore, just discard the result. */
    if (r->state != RAFT_LEADER) {
        tracef("local server is not leader -> ignore write log result");
	evtWarnf("raft(%llx) is not leader, ignore write log result", r->id);
        goto out;
    }

    /* If Check if we have reached a quorum. */
    server_index = configurationIndexOf(&r->configuration, r->id);

    /* Only update the next index if we are part of the current
     * configuration. The only case where this is not true is when we were
     * asked to remove ourselves from the cluster.
     *
     * From Section 4.2.2:
     *
     *   there will be a period of time (while it is committing Cnew) when a
     *   leader can manage a cluster that does not include itself; it
     *   replicates log entries but does not count itself in majorities.
     */
    if (server_index < r->configuration.n) {
        r->leader_state.progress[server_index].match_index = r->last_stored;
    } else {
        const struct raft_entry *entry = logGet(&r->log, r->last_stored);
        assert(entry->type == RAFT_CHANGE);
    }

    /* Check if we can commit some new entries. */
    replicationQuorum(r, r->last_stored);

    rv = replicationApply(r);
    if (rv != 0) {
        evtErrf("raft(%llx) replication apply failed %d", r->id, status);
        /* TODO: just log the error? */
	evtErrf("raft(%llx) apply error %d", r->id, rv);
    }

out:
    if (r->prev_append_status != 0 && status == 0) {
        evtErrf("raft(%llx) previous append status %d", r->id,
		r->prev_append_status);
	abort();
    }

    /* Tell the log that we're done referencing these entries. */
    logRelease(&r->log, request->index, request->entries, request->n);
    if (status != 0 && r->prev_append_status == 0) {
        logTruncate(&r->log, request->index);
        evtErrf("raft(%llx) truncate log from %llu", r->id, request->index);
    }

    r->prev_append_status = status;
    /* Reset prev append status when the last request callback*/
    if (r->prev_append_status != 0 && r->nr_appending_requests == 0) {
        r->prev_append_status = 0;
        evtWarnf("raft(%llx) reset previous append status", r->id);
    }
    raft_free(request);
}

/* Submit a disk write for all entries from the given index onward. */
static int appendLeader(struct raft *r, raft_index index)
{
    struct raft_entry *entries;
    unsigned n;
    struct appendLeader *request;
    int rv;

    assert(r->state == RAFT_LEADER);
    assert(index > 0);
    assert(index > r->last_stored);

    hookRequestAppend(r, index);
    if (r->prev_append_status) {
        evtErrf("raft(%llx) reject append with prev append status %d",
		r->id, r->prev_append_status);
        return r->prev_append_status;
    }

    /* Acquire all the entries from the given index onwards. */
    rv = logAcquire(&r->log, index, &entries, &n);
    if (rv != 0) {
        evtErrf("raft(%llx) log acquire failed %d", r->id, rv);
        goto err;
    }

    /* We expect this function to be called only when there are actually
     * some entries to write. */
    assert(n > 0);

    /* Allocate a new request. */
    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("%s", "malloc");
        goto err_after_entries_acquired;
    }

    request->raft = r;
    request->index = index;
    request->entries = entries;
    request->n = n;
    request->req.data = request;

    r->nr_appending_requests += 1;
    rv = r->io->append(r->io, &request->req, entries, n, appendLeaderCb);
    if (rv != 0) {
        r->nr_appending_requests -= 1;
        evtErrf("raft(%llx) append failed %d", r->id, rv);
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        goto err_after_request_alloc;
    }

    return 0;

err_after_request_alloc:
    raft_free(request);
err_after_entries_acquired:
    logRelease(&r->log, index, entries, n);
err:
    assert(rv != 0);
    return rv;
}

int replicationTrigger(struct raft *r, raft_index index)
{
    int rv;

    rv = appendLeader(r, index);
    if (rv != 0) {
        evtErrf("raft(%llx) append leader failed %d", r->id, rv);
        return rv;
    }

    if (r->state != RAFT_LEADER)
        return 0;

    return triggerAll(r);
}

/* Helper to be invoked after a promotion of a non-voting server has been
 * requested via @raft_assign and that server has caught up with logs.
 *
 * This function changes the local configuration marking the server being
 * promoted as actually voting, appends the a RAFT_CHANGE entry with the new
 * configuration to the local log and triggers its replication. */
static int triggerActualPromotion(struct raft *r)
{
    raft_index index;
    raft_term term = r->current_term;
    size_t server_index;
    struct raft_server *server;
    int old_role;
    int rv;
    const struct raft_entry *entry;

    assert(r->state == RAFT_LEADER);
    assert(r->leader_state.promotee_id != 0);

    server_index =
        configurationIndexOf(&r->configuration, r->leader_state.promotee_id);
    assert(server_index < r->configuration.n);

    server = &r->configuration.servers[server_index];

    assert(server->role != RAFT_VOTER);

    /* Update our current configuration. */
    old_role = server->role;
    server->role = r->leader_state.promotee_role;

    /* Index of the entry being appended. */
    index = logLastIndex(&r->log) + 1;

    /* Encode the new configuration and append it to the log. */
    rv = logAppendConfiguration(&r->log, term, &r->configuration);
    if (rv != 0) {
        evtErrf("raft(%llx) log append conf failed %d", r->id, rv);
        goto err;
    }

    entry = logGet(&r->log, index);
    assert(entry);
    assert(entry->type == RAFT_CHANGE);
    r->hook->entry_after_append_fn(r->hook, index, entry);

    evtNoticef("raft(%llx) promotee %llx promoted to voter ", r->id,
	       r->leader_state.promotee_id);

    /* Start writing the new log entry to disk and send it to the followers. */
    rv = replicationTrigger(r, index);
    if (rv != 0) {
        evtErrf("raft(%llx) replication trigger failed %d", r->id, rv);
        goto err_after_log_append;
    }

    r->leader_state.promotee_id = 0;
    r->leader_state.promotee_role = -1;
    r->configuration_uncommitted_index = logLastIndex(&r->log);

    return 0;

err_after_log_append:
    logTruncate(&r->log, index);

err:
    server->role = old_role;

    assert(rv != 0);
    return rv;
}

int replicationUpdate(struct raft *r,
		      const raft_id id,
                      const struct raft_append_entries_result *result)
{
    bool is_being_promoted;
    raft_index last_index;
    raft_index prev_match_index;
    raft_index match_index;
    unsigned i;
    int rv;

    i = configurationIndexOf(&r->configuration, id);

    assert(r->state == RAFT_LEADER);
    assert(i < r->configuration.n);

    progressMarkRecentRecv(r, i);

    /* If the RPC failed because of a log mismatch, retry.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   - If AppendEntries fails because of log inconsistency:
     *     decrement nextIndex and retry.
     */
    if (result->rejected > 0) {
        evtNoticef("raft(%llx) %llx %d rejected %lu %lu %lu %lu",
		   r->id, id, i, result->rejected, result->last_log_index,
		   result->term, result->pkt);
        bool retry;
        retry = progressMaybeDecrement(r, i, result->rejected,
                                       result->last_log_index);
        if (retry) {
            /* Retry, ignoring errors. */
	    tracef("log mismatch -> send old entries to %llu", id);
            evtNoticef("raft(%llx) send old entries to %llx", r->id, id);
            replicationProgress(r, i);
        }
        return 0;
    }

    /* In case of success the remote server is expected to send us back the
     * value of prevLogIndex + len(entriesToAppend). If it has a longer log, it
     * might be a leftover from previous terms. */
    last_index = result->last_log_index;
    if (last_index > logLastIndex(&r->log)) {
        last_index = logLastIndex(&r->log);
    }

    prev_match_index = progressMatchIndex(r, i);
    /* If the RPC succeeded, update our counters for this server.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   If successful update nextIndex and matchIndex for follower.
     */
    if (!progressMaybeUpdate(r, i, last_index)) {
        return 0;
    }
    match_index = progressMatchIndex(r, i);
    assert(match_index > prev_match_index);
    hookRequestMatch(r, prev_match_index + 1, match_index - prev_match_index,
		     id);

    switch (progressState(r, i)) {
        case PROGRESS__SNAPSHOT:
            /* If a snapshot has been installed, transition back to probe */
            if (progressSnapshotDone(r, i)) {
                progressToProbe(r, i);
            }
            break;
        case PROGRESS__PROBE:
            /* Transition to pipeline */
            progressToPipeline(r, i);
    }

    /* If the server is currently being promoted and is catching with logs,
     * update the information about the current catch-up round, and possibly
     * proceed with the promotion. */
    is_being_promoted = r->leader_state.promotee_id != 0 &&
			r->leader_state.promotee_id == id;
    if (is_being_promoted) {
        bool is_up_to_date = membershipUpdateCatchUpRound(r);
        if (is_up_to_date) {
            rv = triggerActualPromotion(r);
            if (rv != 0) {
                evtErrf("raft(%llx) trigger promotion failed %d", r->id, rv);
                return rv;
            }
        }
    }

    /* Check if we can commit some new entries. */
    replicationQuorum(r, min(last_index, r->last_stored));

    rv = replicationApply(r);
    if (rv != 0) {
        evtErrf("raft(%llx) replication apply failed %d", r->id, rv);
        /* TODO: just log the error? */
    }

    /* Abort here we have been removed and we are not leaders anymore. */
    if (r->state != RAFT_LEADER) {
        goto out;
    }

    /* Get again the server index since it might have been removed from the
     * configuration. */
    i = configurationIndexOf(&r->configuration, id);

    if (i < r->configuration.n) {
        /* If we are transferring leadership to this follower, check if its log
         * is now up-to-date and, if so, send it a TimeoutNow RPC (unless we
         * already did). */
	if (r->transfer != NULL && r->transfer->id == id) {
            if (progressIsUpToDate(r, i) && r->transfer->send.data == NULL) {
                rv = membershipLeadershipTransferStart(r);
                if (rv != 0) {
                    membershipLeadershipTransferClose(r);
                }
            }
        }
        /* If this follower is in pipeline mode, send it more entries. */
        if (progressState(r, i) == PROGRESS__PIPELINE) {
            replicationProgress(r, i);
        }
    }

out:
    return 0;
}

static void sendAppendEntriesResultCb(struct raft_io_send *req, int status)
{
    (void)status;
    HeapFree(req);
}

static void sendAppendEntriesResult(
    struct raft *r,
    const struct raft_append_entries_result *result)
{
    struct raft_message message;
    struct raft_io_send *req;
    int rv;

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = r->follower_state.current_leader.id;
    message.append_entries_result = *result;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        evtErrf("%s", "malloc");
        return;
    }
    req->data = r;

    rv = r->io->send(r->io, req, &message, sendAppendEntriesResultCb);
    if (rv != 0) {
        if (rv != RAFT_NOCONNECTION)
            evtErrf("raft(%llx) send failed %d", r->id, rv);
        raft_free(req);
    }
}

/* Context for a write log entries request that was submitted by a follower. */
struct appendFollower
{
    struct raft *raft; /* Instance that has submitted the request */
    raft_index index;  /* Index of the first entry in the request. */
    struct raft_append_entries args;
    struct raft_io_append req;
};

static void appendFollowerCb(struct raft_io_append *req, int status)
{
    struct appendFollower *request = req->data;
    struct raft *r = request->raft;
    struct raft_append_entries *args = &request->args;
    struct raft_append_entries_result result;
    size_t i;
    size_t j;
    int rv;

    tracef("I/O completed on follower: status %d", status);

    assert(args->entries != NULL);
    assert(args->n_entries > 0);

    assert(r->nr_appending_requests > 0);
    r->nr_appending_requests -= 1;

    result.pkt = args->pkt;
    result.term = r->current_term;
    if (status != 0) {
        evtErrf("raft(%llx) append follower status %d", r->id, status);
        if (r->state != RAFT_FOLLOWER) {
            tracef("local server is not follower -> ignore I/O failure");
            goto out;
        }
        result.rejected = args->prev_log_index + 1;
        goto respond;
    }

    /* If we're shutting down or have errored, ignore the result. */
    if (r->state == RAFT_UNAVAILABLE) {
        tracef("local server is unavailable -> ignore I/O result");
        goto out;
    }

    /* We received an InstallSnapshot RCP while these entries were being
     * persisted to disk */
    if (replicationInstallSnapshotBusy(r)) {
        goto out;
    }

    i = updateLastStored(r, request->index, args->entries, args->n_entries);
    if(r->enable_dynamic_trailing){
        raft_set_snapshot_trailing(r, args->trailing);
    }  

    /* If none of the entries that we persisted is present anymore in our
     * in-memory log, there's nothing to report or to do. We just discard
     * them. */
    if (i == 0 || r->state != RAFT_FOLLOWER) {
        goto out;
    }

    /* Possibly apply configuration changes as uncommitted. */
    for (j = 0; j < i; j++) {
        struct raft_entry *entry = &args->entries[j];
        raft_index index = request->index + j;
        raft_term local_term = logTermOf(&r->log, index);

        assert(local_term != 0 && local_term == entry->term);

        if (entry->type == RAFT_CHANGE) {
            rv = membershipUncommittedChange(r, index, entry);
            if (rv != 0) {
                evtErrf("raft(%llx) ship change failed %d", r->id, rv);
                goto out;
            }
        }
    }

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: If leaderCommit >
     *   commitIndex, set commitIndex = min(leaderCommit, index of last new
     *   entry).
     */
    if (args->leader_commit > r->commit_index) {
        r->commit_index = min(args->leader_commit, r->last_stored);
        rv = replicationApply(r);
        if (rv != 0) {
            evtErrf("raft(%llx) replication apply failed %d", r->id, rv);
            goto out;
        }
    }

    if (r->state != RAFT_FOLLOWER) {
        tracef("local server is not follower -> don't send result");
        goto out;
    }

    result.rejected = 0;

respond:
    result.last_log_index = r->last_stored;
    sendAppendEntriesResult(r, &result);

out:
    if (r->prev_append_status != 0 && status == 0) {
        evtErrf("raft(%llx) previous append status %d", r->id,
		r->prev_append_status);
       abort();
    }

    logRelease(&r->log, request->index, request->args.entries,
	    request->args.n_entries);

    if (status != 0 && r->prev_append_status == 0) {
        logTruncate(&r->log, request->index);
        evtErrf("raft(%llx) truncate log from %llu", r->id, request->index);
    }

    r->prev_append_status = status;
    /* Reset prev append status when the last request callback*/
    if (r->prev_append_status != 0 && r->nr_appending_requests == 0) {
        r->prev_append_status = 0;
        evtWarnf("raft(%llx) reset previous append status", r->id);
    }

    raft_free(request);
}

/* Check the log matching property against an incoming AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   2. Reply false if log doesn't contain an entry at prevLogIndex whose
 *   term matches prevLogTerm.
 *
 * Return 0 if the check passed.
 *
 * Return 1 if the check did not pass and the request needs to be rejected.
 *
 * Return -1 if there's a conflict and we need to shutdown. */
static int checkLogMatchingProperty(struct raft *r,
                                    const struct raft_append_entries *args)
{
    raft_term local_prev_term;

    /* If this is the very first entry, there's nothing to check. */
    if (args->prev_log_index == 0) {
        return 0;
    }

    local_prev_term = logTermOf(&r->log, args->prev_log_index);
    if (local_prev_term == 0) {
        tracef("no entry at index %llu -> reject", args->prev_log_index);
        return 1;
    }

    if (local_prev_term != args->prev_log_term) {
        if (args->prev_log_index <= r->commit_index) {
            /* Should never happen; something is seriously wrong! */
            evtErrf(
		"raft(%llx) conflicting terms %llu and %llu for entry %llu"
		"(commit index %llu) -> shutdown",
                r->id, local_prev_term, args->prev_log_term,
		args->prev_log_index, r->commit_index);
            return -1;
        }
        tracef("previous term mismatch -> reject");
        return 1;
    }

    return 0;
}

/* Delete from our log all entries that conflict with the ones in the given
 * AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   3. If an existing entry conflicts with a new one (same index but
 *   different terms), delete the existing entry and all that follow it.
 *
 * The i output parameter will be set to the array index of the first new log
 * entry that we don't have yet in our log, among the ones included in the given
 * AppendEntries request. */
static int deleteConflictingEntries(struct raft *r,
                                    const struct raft_append_entries *args,
                                    size_t *i)
{
    size_t j;
    int rv;

    for (j = 0; j < args->n_entries; j++) {
        struct raft_entry *entry = &args->entries[j];
        raft_index entry_index = args->prev_log_index + 1 + j;
        raft_term local_term = logTermOf(&r->log, entry_index);

        if (local_term > 0 && local_term != entry->term) {
            if (entry_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                tracef("new index conflicts with committed entry -> shutdown");
                evtErrf("raft(%llx) entry conflicts %llu/%llu %llu/%llu",
			r->id, entry_index, r->commit_index, local_term,
			entry->term);
                return RAFT_SHUTDOWN;
            }

            tracef("log mismatch -> truncate (%llu)", entry_index);

            /* Possibly discard uncommitted configuration changes. */
            if (r->configuration_uncommitted_index >= entry_index) {
                rv = membershipRollback(r);
                if (rv != 0) {
                    evtErrf("raft(%llx) rollback failed %d", r->id, rv);
                    return rv;
                }
            }

            /* Delete all entries from this index on because they don't
             * match. */
            rv = r->io->truncate(r->io, entry_index);
            if (rv != 0) {
                evtErrf("raft(%llx) truncate failed %d", r->id, rv);
                return rv;
            }
            logTruncate(&r->log, entry_index);

            /* Drop information about previously stored entries that have just
             * been discarded. */
            if (r->last_stored >= entry_index) {
                r->last_stored = entry_index - 1;
            }

            /* We want to append all entries from here on, replacing anything
             * that we had before. */
            break;
        } else if (local_term == 0) {
            /* We don't have an entry at this index, so we want to append this
             * new one and all the subsequent ones. */
            break;
        }
    }

    *i = j;

    return 0;
}

int replicationAppend(struct raft *r,
                      const struct raft_append_entries *args,
                      raft_index *rejected,
                      bool *async)
{
    struct appendFollower *request;
    int match;
    size_t n;
    size_t i;
    size_t j;
    int rv;

    assert(r != NULL);
    assert(args != NULL);
    assert(rejected != NULL);
    assert(async != NULL);

    assert(r->state == RAFT_FOLLOWER);

    *rejected = args->prev_log_index;
    *async = false;

    if (args->prev_log_index == 0) {
        evtNoticef("raft(%llx) recv term %llu entries %lu %llu %llu %llu",
		   r->id, args->term, args->n_entries, args->prev_log_index,
		   args->prev_log_term, args->leader_commit);
	evtNoticef("raft(%llx) snapshot %llu last log %llu",
		   r->id, r->log.snapshot.last_index, logLastIndex(&r->log));
    }

    /* Check the log matching property. */
    match = checkLogMatchingProperty(r, args);
    if (match != 0) {
        assert(match == 1 || match == -1);
        return match == 1 ? 0 : RAFT_SHUTDOWN;
    }

    /* Delete conflicting entries. */
    rv = deleteConflictingEntries(r, args, &i);
    if (rv != 0) {
        evtErrf("raft(%llx) delete conflicting entries failed %d", r->id, rv);
        return rv;
    }

    *rejected = 0;

    n = args->n_entries - i; /* Number of new entries */

    /* If this is an empty AppendEntries, there's nothing to write. However we
     * still want to check if we can commit some entry. However, don't commit
     * anything while a snapshot install is busy, r->last_stored will be 0 in
     * that case.
     *
     * From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: If leaderCommit >
     *   commitIndex, set commitIndex = min(leaderCommit, index of last new
     *   entry).
     */
    if (n == 0) {
        if ((args->leader_commit > r->commit_index)
             && !replicationInstallSnapshotBusy(r)) {
            r->commit_index = min(args->leader_commit, r->last_stored);
            rv = replicationApply(r);
            if (rv != 0) {
                evtErrf("raft(%llx) replication apply failed %d", r->id, rv);
                return rv;
            }
        }

        return 0;
    }

    *async = true;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("%s", "malloc");
        goto err;
    }

    request->raft = r;
    request->args = *args;
    /* Index of first new entry */
    request->index = args->prev_log_index + 1 + i;

    /* Update our in-memory log to reflect that we received these entries. We'll
     * notify the leader of a successful append once the write entries request
     * that we issue below actually completes.  */
    for (j = 0; j < n; j++) {
        struct raft_entry *entry = &args->entries[i + j];
         /* TODO This copy should not strictly be necessary, as the batch logic will
          * take care of freeing the batch buffer in which the entries are received.
          * However, this would lead to memory spikes in certain edge cases.
          * https://github.com/canonical/dqlite/issues/276
          */
	rv = logAppend(&r->log, entry->term, entry->type, &entry->buf, entry->batch);
        if (rv != 0) {
            evtErrf("raft(%llx) log append failed %d", r->id, rv);
            goto err_after_request_alloc;
        }
    }

    /* Acquire the relevant entries from the log. */
    rv = logAcquire(&r->log, request->index, &request->args.entries,
                    &request->args.n_entries);
    if (rv != 0) {
        evtErrf("raft(%llx) log acquire failed %d", r->id, rv);
        goto err_after_request_alloc;
    }

    assert(request->args.n_entries == n);
    if (request->args.n_entries == 0) {
	    tracef("No log entries found at index %llu", request->index);
	    tracef("args->prev_log_term = %llu, args->prev_log_index = %llu, args->n_entries = %u",
		   args->prev_log_term, args->prev_log_index, args->n_entries);
	    ErrMsgPrintf(r->errmsg, "No log entries found at index %llu", request->index);
	    evtErrf("raft(%llx) no log entries found at index %llu",
		    r->id, request->index);
	    rv = RAFT_SHUTDOWN;
	    goto err_after_acquire_entries;
    }
    r->nr_appending_requests += 1;
    request->req.data = request;
    rv = r->io->append(r->io, &request->req, request->args.entries,
                       request->args.n_entries, appendFollowerCb);
    if (rv != 0) {
        r->nr_appending_requests -= 1;
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        evtErrf("raft(%llx) append failed %d", r->id, rv);
        goto err_after_acquire_entries;
    }

    entryNonBatchDestroyPrefix(args->entries, args->n_entries, i);
    raft_free(args->entries);
    return 0;

err_after_acquire_entries:
    /* Release the entries related to the IO request */
    logRelease(&r->log, request->index, request->args.entries,
               request->args.n_entries);

err_after_request_alloc:
    /* Release all entries added to the in-memory log, making
     * sure the in-memory log and disk don't diverge, leading
     * to future log entries not being persisted to disk.
     */
    if (j != 0) {
        logDiscard(&r->log, request->index);
    }
    raft_free(request);

err:
    assert(rv != 0);
    return rv;
}

struct recvInstallSnapshot
{
    struct raft *raft;
    struct raft_snapshot snapshot;
};

static void installSnapshotCb(struct raft_io_snapshot_put *req, int status)
{
    struct recvInstallSnapshot *request = req->data;
    struct raft *r = request->raft;
    struct raft_snapshot *snapshot = &request->snapshot;
    struct raft_append_entries_result result;
    int rv;

    r->snapshot.put.data = NULL;

    result.term = r->current_term;

    /* If we are shutting down, let's discard the result. TODO: what about other
     * states? */
    if (r->state == RAFT_UNAVAILABLE) {
        goto discard;
    }

    if (status != 0) {
        result.rejected = snapshot->index;
        tracef("save snapshot %llu: %s", snapshot->index,
               raft_strerror(status));
        goto discard;
    }

    /* From Figure 5.3:
     *
     *   7. Discard the entire log
     *   8. Reset state machine using snapshot contents (and load lastConfig
     *      as cluster configuration).
     */
    rv = snapshotRestore(r, snapshot);
    if (rv != 0) {
        result.rejected = snapshot->index;
        tracef("restore snapshot %llu: %s", snapshot->index,
               raft_strerror(status));
        goto discard;
    }

    tracef("restored snapshot with last index %llu", snapshot->index);

    result.rejected = 0;

    goto respond;

discard:
    /* In case of error we must also free the snapshot data buffer and free the
     * configuration. */
    raft_free(snapshot->bufs[0].base);
    raft_configuration_close(&snapshot->configuration);

respond:
    if (r->state != RAFT_UNAVAILABLE) {
        result.last_log_index = r->last_stored;
        sendAppendEntriesResult(r, &result);
    }

    raft_free(request);
}

int replicationInstallSnapshot(struct raft *r,
                               const struct raft_install_snapshot *args,
                               raft_index *rejected,
                               bool *async)
{
    struct recvInstallSnapshot *request;
    struct raft_snapshot *snapshot;
    raft_term local_term;
    int rv;

    assert(r->state == RAFT_FOLLOWER);

    *rejected = args->last_index;
    *async = false;

    /* If we are taking a snapshot ourselves or installing a snapshot, ignore
     * the request, the leader will eventually retry. TODO: we should do
     * something smarter. */
    if (r->snapshot.pending.term != 0 || r->snapshot.put.data != NULL) {
        *async = true;
        return RAFT_BUSY;
    }

    /* If our last snapshot is more up-to-date, this is a no-op */
    if (r->log.snapshot.last_index >= args->last_index) {
        *rejected = 0;
        return 0;
    }

    /* If we already have all entries in the snapshot, this is a no-op */
    local_term = logTermOf(&r->log, args->last_index);
    if (local_term != 0 && local_term >= args->last_term) {
        *rejected = 0;
        return 0;
    }

    *async = true;

    /* Preemptively update our in-memory state. */
    logRestore(&r->log, args->last_index, args->last_term);

    r->last_stored = 0;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        evtErrf("%s", "malloc");
        rv = RAFT_NOMEM;
        goto err;
    }
    request->raft = r;

    snapshot = &request->snapshot;
    snapshot->term = args->last_term;
    snapshot->index = args->last_index;
    snapshot->configuration_index = args->conf_index;
    snapshot->configuration = args->conf;

    snapshot->bufs = raft_malloc(sizeof *snapshot->bufs);
    if (snapshot->bufs == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("%s", "malloc");
        goto err_after_request_alloc;
    }
    snapshot->bufs[0] = args->data;
    snapshot->n_bufs = 1;

    assert(r->snapshot.put.data == NULL);
    r->snapshot.put.data = request;
    rv = r->io->snapshot_put(r->io,
                             0 /* zero trailing means replace everything */,
                             &r->snapshot.put, snapshot, installSnapshotCb);
    if (rv != 0) {
        evtErrf("raft(%llx) snapshot put failed %d", r->id, rv);
        goto err_after_bufs_alloc;
    }

    return 0;

err_after_bufs_alloc:
    raft_free(snapshot->bufs);
    r->snapshot.put.data = NULL;
err_after_request_alloc:
    raft_free(request);
err:
    assert(rv != 0);
    return rv;
}

struct applyCmd
{
    struct raft *raft;          	/* Instance that has submitted the request */
    raft_index index;           	/* Index of the first entry in the request. */
    struct raft_fsm_apply req;
};
static void applyCommandCb(struct raft_fsm_apply *req,
                           void *result,
                           int status)
{
    struct applyCmd *request = req->data;
    struct raft *r = request->raft;
    raft_index index = request->index;
    struct raft_apply *creq;

    assert((r->last_applied + 1) == index);
    hookRequestApplyDone(r, index);

    creq = (struct raft_apply *)getRequest(r, index);
    if (creq != NULL && creq->cb != NULL) {
        assert(creq->type == RAFT_COMMAND);
        creq->cb(creq, status, result);
    }
    r->last_applied = index;
    raft_free(request);

    if (r->last_applied == r->last_applying) {
        if (r->state == RAFT_LEADER ||
                r->state == RAFT_FOLLOWER) {
            int rv = replicationApply(r);

            if (rv != 0) {
                evtErrf("raft(%llx) replication apply failed %d", r->id, rv);
                /* TODO: just log the error? */
            }
        }
    }
}
/* Apply a RAFT_COMMAND entry that has been committed. */
static int applyCommand(struct raft *r,
                        const raft_index index,
                        const struct raft_buffer *buf)
{
    struct applyCmd *request;
    int rv;

    assert(index > 0);
    assert(index <= r->commit_index);

    hookRequestApply(r, index);
    request = raft_malloc(sizeof(*request));
    if (request == NULL)
        return RAFT_NOMEM;

    request->raft = r;
    request->index = index;
    request->req.data = request;

    rv = r->fsm->apply(r->fsm,
                       &request->req,
                       buf,
                       applyCommandCb);
    if (rv != 0) {
        evtErrf("raft(%llx) apply failed %d", r->id, rv);
        raft_free(request);
    }

    return rv;
}

/* Fire the callback of a barrier request whose entry has been committed. */
static void applyBarrier(struct raft *r, const raft_index index)
{
    struct raft_barrier *req;

    hookRequestApply(r, index);
    hookRequestApplyDone(r, index);
    req = (struct raft_barrier *)getRequest(r, index);
    if (req != NULL && req->cb != NULL) {
        assert(req->type == RAFT_BARRIER);
        req->cb(req, 0);
    }
}

/* Apply a RAFT_CHANGE entry that has been committed. */
static void applyChange(struct raft *r, const raft_index index)
{
    struct raft_change *req;

    assert(index > 0);

    /* If this is an uncommitted configuration that we had already applied when
     * submitting the configuration change (for leaders) or upon receiving it
     * via an AppendEntries RPC (for followers), then reset the uncommitted
     * index, since that uncommitted configuration is now committed. */
    if (r->configuration_uncommitted_index == index) {
        r->configuration_uncommitted_index = 0;
        r->configuration_index = index;

        if (r->state == RAFT_LEADER) {
            const struct raft_server *server;
            req = r->leader_state.change;
            r->leader_state.change = NULL;

        /* If we are leader but not part of this new configuration, step
         * down.
         *
         * From Section 4.2.2:
         *
         *   In this approach, a leader that is removed from the configuration
         *   steps down once the Cnew entry is committed.
         */
            server = configurationGet(&r->configuration, r->id);
            if (server == NULL) {
	        evtNoticef("raft(%llx) not in configuration", r->id);
		evtDumpConfiguration(r, &r->configuration);
                convertToFollower(r);
            }

            if (req != NULL && req->cb != NULL) {
                req->cb(req, 0);
            }
        }
    }
}

static raft_index nextSnapshotIndex(struct raft *r)
{
	raft_index snapshot_index = r->last_applied;
	raft_index hook_snapshot_index = 0;

	if (r->hook->get_next_snapshot_index)
		hook_snapshot_index = r->hook->get_next_snapshot_index(r->hook);

	if (r->state == RAFT_LEADER && r->sync_replication) {
		progressUpdateMinMatch(r);
		snapshot_index = min(r->leader_state.min_match_index,
				     r->last_applied);
	}

	if (r->state == RAFT_FOLLOWER && r->sync_snapshot) {
		snapshot_index =
			min(r->follower_state.current_leader.snapshot_index,
			    r->last_applied);
	}

	if (hook_snapshot_index != 0)
		snapshot_index = min(hook_snapshot_index, snapshot_index);

	return snapshot_index;
}

static bool shouldTakeSnapshot(struct raft *r)
{
    raft_index snapshot_index;
    struct raft_server *s;
    struct raft_progress *p;
    raft_index tmp = logLastIndex(&r->log);
    bool ignoreUpdateTrailing = false;
    unsigned i;
    /* If we are shutting down, let's not do anything. */
    if (r->state == RAFT_UNAVAILABLE) {
        return false;
    }

    /* If we are changing the configuration, do nothing */
    if (r->configuration_uncommitted_index != 0) {
        return false;
    }

    /* If a snapshot is already in progress or we're installing a snapshot, we
     * don't want to start another one. */
    if (r->snapshot.pending.term != 0 || r->snapshot.put.data != NULL) {
        return false;
    }

    for (i = 0; i<r->configuration.n; i++) {
        if(r->state != RAFT_LEADER) 
            break;
        s = &r->configuration.servers[i];
        if (s->role == RAFT_SPARE &&
			s->id != r->leader_state.promotee_id) {
			continue;
		}
        p = &r->leader_state.progress[i];
        if (p->match_index <= tmp) {
			tmp = p->match_index;
            raft_time now = r->io->time(r->io);
            
            if(now - p->recent_recv_time > r->reset_trailing_timeout){
                raft_set_snapshot_trailing(r, r->snapshot.threshold);
                if(r->log.snapshot.last_index > 0)
                    logSnapshot(&r->log, r->log.snapshot.last_index, r->snapshot.trailing);
                ignoreUpdateTrailing = true;
            }
        }
    }

    snapshot_index = nextSnapshotIndex(r);
    if (snapshot_index <= r->log.snapshot.last_index)
	    return false;
    /* If we didn't reach the threshold yet, do nothing. */
    if (snapshot_index - r->log.snapshot.last_index < r->snapshot.threshold) {
        return false;
    }
    evtInfof("raft(%llx) ignoreUpdateTrailing %d enable_dynamic_trailing %d", r->id, 
                ignoreUpdateTrailing, r->enable_dynamic_trailing);
    if (ignoreUpdateTrailing == false && r->enable_dynamic_trailing) {
        if (r->state == RAFT_LEADER) {
            r->snapshot.trailing = 0;
            do{
                r->snapshot.trailing += r->snapshot.threshold;
                tmp += r->snapshot.threshold;
            }while(tmp < snapshot_index);
        } else {
            r->snapshot.trailing += r->snapshot.threshold;
        }
        evtInfof("raft(%llx) update snapshot trailing to %u", r->id, r->snapshot.trailing);
    }
    //因为tmp和ignoreUpdateTrailing都是Leader状态下才会改变的值，Leader通过上面这个修改很正常
    //那么非Leader的该怎么改呢，主动扩展？非主动扩展？
    //主动扩展，可以容纳下之前的，但有可能会扩展得很大？非主动扩展有可能Leader还没丢，但是你主动丢了？
    
    return true;
}

static void takeSnapshotCb(struct raft_io_snapshot_put *req, int status)
{
    struct raft *r = req->data;
    struct raft_snapshot *snapshot;

    r->snapshot.put.data = NULL;
    snapshot = &r->snapshot.pending;

    if (status != 0) {
        tracef("snapshot %lld at term %lld: %s", snapshot->index,
               snapshot->term, raft_strerror(status));
        evtErrf("raft(%llx) take snapshot %llu/%llu failed %d", snapshot->index,
		snapshot->term, status);
        goto out;
    }
    /* backup the configuration in case a membershipRollback occurs */
    configurationClose(&r->snapshot.configuration);
    status = configurationCopy(&r->snapshot.pending.configuration,
                               &r->snapshot.configuration);
    if (status != 0) {
        tracef("snapshot %lld at term %lld: %s, failed to backup the configuration ",
               snapshot->index,
               snapshot->term,
               raft_strerror(status));
        evtErrf("raft(%llx) copy conf failed %d", r->id, status);
        goto out;
    }

    evtInfof("raft(%llx) take snapshot at %llu %u enable %d", r->id, snapshot->index,
	     r->snapshot.trailing, r->enable_free_trailing);
    logSnapshot(&r->log, snapshot->index, r->snapshot.trailing);
    if (r->enable_free_trailing) {
        freeEntriesBufForward(&r->log, snapshot->index);
    }
    if (r->enable_dynamic_trailing) {
        raft_set_snapshot_trailing(r, r->snapshot.threshold + r->snapshot.trailing);
        evtInfof("raft(%llx) update snapshot trailing to %u", r->id, r->snapshot.trailing);
    }
out:
    snapshotClose(&r->snapshot.pending);
    r->snapshot.pending.term = 0;
}

static int takeSnapshot(struct raft *r)
{
    struct raft_snapshot *snapshot;
    unsigned i;
    int rv;
    raft_index snapshot_index = nextSnapshotIndex(r);

    tracef("take snapshot at %lld", snapshot_index);

    snapshot = &r->snapshot.pending;
    snapshot->index = snapshot_index;
    snapshot->term = logTermOf(&r->log, snapshot_index);

    rv = configurationCopy(&r->configuration, &snapshot->configuration);
    if (rv != 0) {
        evtErrf("raft(%llx) copy conf failed %d", r->id, rv);
        goto abort;
    }

    snapshot->configuration_index = r->configuration_index;

    rv = r->fsm->snapshot(r->fsm, &snapshot->bufs, &snapshot->n_bufs);
    if (rv != 0) {
        evtErrf("raft(%llx) snapshot failed %d", r->id, rv);
        /* Ignore transient errors. We'll retry next time. */
        if (rv == RAFT_BUSY) {
            rv = 0;
        }
        goto abort_after_config_copy;
    }

    assert(r->snapshot.put.data == NULL);
    r->snapshot.put.data = r;
    rv = r->io->snapshot_put(r->io, r->snapshot.trailing, &r->snapshot.put,
                             snapshot, takeSnapshotCb);
    if (rv != 0) {
        evtErrf("raft(%llx) snapshot put failed %d", r->id, rv);
        goto abort_after_fsm_snapshot;
    }

    return 0;

abort_after_fsm_snapshot:
    for (i = 0; i < snapshot->n_bufs; i++) {
        raft_free(snapshot->bufs[i].base);
    }
    raft_free(snapshot->bufs);
abort_after_config_copy:
    raft_configuration_close(&snapshot->configuration);
abort:
    r->snapshot.pending.term = 0;
    r->snapshot.put.data = NULL;
    return rv;
}

int replicationApply(struct raft *r)
{
    raft_index index;
    int rv = 0;

    assert(r->state == RAFT_LEADER || r->state == RAFT_FOLLOWER);
    assert(r->last_applied <= r->commit_index);

    if (r->last_applied == r->commit_index) {
        /* Nothing to do. */
        goto err_take_snapshot;
    }

    while(r->last_applying < r->commit_index) {
        index = r->last_applying + 1;
        const struct raft_entry *entry = logGet(&r->log, index);
        if (entry == NULL) {
            /* This can happen while installing a snapshot */
            tracef("replicationApply - ENTRY NULL");
            return 0;
        }

        assert(entry->type == RAFT_COMMAND || entry->type == RAFT_BARRIER ||
               entry->type == RAFT_CHANGE);

        switch (entry->type) {
            case RAFT_COMMAND:
                r->last_applying = index;
                rv = applyCommand(r, index, &entry->buf);
                if (rv != 0)
                    r->last_applying -= 1;
                break;
            case RAFT_BARRIER:
                if (r->last_applying > r->last_applied)
                    return 0;
                r->hook->entry_after_apply_fn(r->hook, index, entry);
                applyBarrier(r, index);
                r->last_applied = index;
                r->last_applying = index;
                break;
            case RAFT_CHANGE:
                if (r->last_applying > r->last_applied)
                    return 0;
                r->hook->entry_after_apply_fn(r->hook, index, entry);
                applyChange(r, index);
                r->last_applied = index;
                r->last_applying = index;
                break;
            default:/* For coverity. This case can't be taken. */
                break;
        }

        if (rv != 0) {
            evtErrf("raft(%llx) apply failed %d %lu", r->id, rv, index);
            break;
        }
    }

err_take_snapshot:
    if (shouldTakeSnapshot(r)) {
        rv = takeSnapshot(r);
	if (rv != 0)
            evtErrf("raft(%llx) take snapshot failed %d", r->id, rv);
    }

    return rv;
}

void replicationQuorum(struct raft *r, const raft_index index)
{
    size_t votes = 0;
    size_t i;
    size_t n_voters;
    raft_index prev_commit_index = r->commit_index;

    assert(r->state == RAFT_LEADER);

    if (index <= r->commit_index) {
        return;
    }
    // assert(logTermOf(&r->log, index) > 0);
    assert(logTermOf(&r->log, index) <= r->current_term);
    /*
     * From section 3.6.2:
     * Raft never commits log entries from previous terms by counting replicas.
     * Only log entries from the leader’s current term are committed
     * by counting replicas.
     */
    if (logTermOf(&r->log, index) < r->current_term) {
        return;
    }

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (server->role != RAFT_VOTER && server->role != RAFT_LOGGER) {
            continue;
        }
        if (r->leader_state.progress[i].match_index >= index) {
            votes++;
        }
    }

    n_voters = configurationVoterCount(&r->configuration);
    if (r->quorum == RAFT_MAJORITY && votes <= n_voters / 2)
	    return;
    if (r->quorum == RAFT_FULL && votes < n_voters)
	    return;

    r->commit_index = index;
    tracef("new commit index %llu", r->commit_index);

    assert(r->commit_index > prev_commit_index);
    hookRequestCommit(r, prev_commit_index + 1,
		      r->commit_index - prev_commit_index);
}

inline bool replicationInstallSnapshotBusy(struct raft *r)
{
    return r->last_stored == 0 && r->snapshot.put.data != NULL;
}

#undef tracef
