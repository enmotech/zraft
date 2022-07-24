#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "tracing.h"
#include "event.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

int raft_apply(struct raft *r,
               struct raft_apply *req,
               const struct raft_buffer bufs[],
               const unsigned n,
               raft_apply_cb cb)
{
    raft_index index;
    int rv;

    assert(r != NULL);
    assert(bufs != NULL);
    assert(n > 0);

    if (r->state != RAFT_LEADER || r->transfer != NULL) {
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
	evtErrf("raft(%16llx) apply failed %d", r->id, rv);
        goto err;
    }

    /* Index of the first entry being appended. */
    index = logLastIndex(&r->log) + 1;
    tracef("%u commands starting at %lld", n, index);
    req->type = RAFT_COMMAND;
    req->index = index;
    req->cb = cb;

    /* Append the new entries to the log. */
    rv = logAppendCommands(&r->log, r->current_term, bufs, n);
    if (rv != 0) {
        evtErrf("raft(%16llx) append cmd failed %d", r->id, rv);
        goto err;
    }

    QUEUE_PUSH(&r->leader_state.requests, &req->queue);

    rv = replicationTrigger(r, index);
    if (rv != 0) {
        evtErrf("raft(%16llx) replication trigger failed %d", r->id, rv);
        goto err_after_log_append;
    }

    return 0;

err_after_log_append:
    logDiscard(&r->log, index);
    QUEUE_REMOVE(&req->queue);
err:
    assert(rv != 0);
    return rv;
}

int raft_barrier(struct raft *r, struct raft_barrier *req, raft_barrier_cb cb)
{
    raft_index index;
    struct raft_buffer buf;
    const struct raft_entry *entry;
    int rv;

    if (r->state != RAFT_LEADER || r->transfer != NULL) {
        rv = RAFT_NOTLEADER;
        evtErrf("raft(%16llx) apply barrier failed %d", r->id, rv);
        goto err;
    }

    /* TODO: use a completely empty buffer */
    buf.len = 0;
    buf.base = NULL;

    /* Index of the barrier entry being appended. */
    index = logLastIndex(&r->log) + 1;
    tracef("barrier starting at %lld", index);
    req->type = RAFT_BARRIER;
    req->index = index;
    req->cb = cb;

    rv = logAppend(&r->log, r->current_term, RAFT_BARRIER, &buf, NULL);
    if (rv != 0) {
        evtErrf("raft(%16llx) append barrier failed %d", r->id, rv);
        goto err_after_buf_alloc;
    }
    QUEUE_PUSH(&r->leader_state.requests, &req->queue);

    entry = logGet(&r->log, index);
    assert(entry);
    assert(entry->type == RAFT_BARRIER);
    r->hook->entry_after_append_fn(r->hook, index, entry);

    rv = replicationTrigger(r, index);
    if (rv != 0) {
        evtErrf("raft(%16llx) replication trigger failed %d", r->id, rv);
        goto err_after_log_append;
    }

    return 0;

err_after_log_append:
    logDiscard(&r->log, index);
    QUEUE_REMOVE(&req->queue);
err_after_buf_alloc:
err:
    return rv;
}

static int clientChangeConfiguration(
    struct raft *r,
    struct raft_change *req,
    const struct raft_configuration *configuration)
{
    raft_index index;
    raft_term term = r->current_term;
    int rv;
    const struct raft_entry *entry;

    (void)req;

    /* Index of the entry being appended. */
    index = logLastIndex(&r->log) + 1;

    /* Encode the new configuration and append it to the log. */
    rv = logAppendConfiguration(&r->log, term, configuration);
    if (rv != 0) {
        evtErrf("raft(%16llx) append conf failed %d", r->id, rv);
        goto err;
    }

    entry = logGet(&r->log, index);
    assert(entry);
    assert(entry->type == RAFT_CHANGE);
    r->hook->entry_after_append_fn(r->hook, index, entry);

    if (configuration->n != r->configuration.n) {
        rv = progressRebuildArray(r, configuration);
        if (rv != 0) {
            evtErrf("raft(%16llx) rebuild array failed %d", r->id, rv);
            goto err;
        }
    }

    /* Update the current configuration if we've created a new object. */
    if (configuration != &r->configuration) {
        raft_configuration_close(&r->configuration);
        r->configuration = *configuration;
    }

    /* Start writing the new log entry to disk and send it to the followers. */
    rv = replicationTrigger(r, index);
    if (rv != 0) {
        evtErrf("raft(%16llx) replication trigger failed %d", r->id, rv);
        /* TODO: restore the old next/match indexes and configuration. */
        goto err_after_log_append;
    }

    r->configuration_uncommitted_index = index;

    return 0;

err_after_log_append:
    logTruncate(&r->log, index);

err:
    assert(rv != 0);
    return rv;
}

int raft_add(struct raft *r,
             struct raft_change *req,
             raft_id id,
             raft_change_cb cb)
{
    struct raft_configuration configuration;
    int rv;

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        evtNoticef("raft(%16llx) change conf failed %d", r->id, rv);
        return rv;
    }

    tracef("add server: id %llu", id);

    /* Make a copy of the current configuration, and add the new server to
     * it. */
    rv = configurationCopy(&r->configuration, &configuration);
    if (rv != 0) {
        evtErrf("raft(%16llx) copy conf failed %d", r->id, rv);
        goto err;
    }

    rv = raft_configuration_add(&configuration, id, RAFT_SPARE);
    if (rv != 0) {
        evtErrf("raft(%16llx) add conf failed %d", r->id, rv);
        goto err_after_configuration_copy;
    }

    req->cb = cb;

    rv = clientChangeConfiguration(r, req, &configuration);
    if (rv != 0) {
        evtErrf("raft(%16llx) change conf failed %d", r->id, rv);
        goto err_after_configuration_copy;
    }
    progressUpdateMinMatch(r);

    assert(r->leader_state.change == NULL);
    r->leader_state.change = req;

    return 0;

err_after_configuration_copy:
    raft_configuration_close(&configuration);
err:
    assert(rv != 0);
    return rv;
}

int raft_assign(struct raft *r,
                struct raft_change *req,
                raft_id id,
                int role,
                raft_change_cb cb)
{
    const struct raft_server *server;
    unsigned server_index;
    raft_index last_index;
    int rv;

    if (role != RAFT_STANDBY && role != RAFT_VOTER && role != RAFT_SPARE) {
        rv = RAFT_BADROLE;
        ErrMsgFromCode(r->errmsg, rv);
        evtErrf("raft(%16llx) assign role %d failed", r->id, role, rv);
        return rv;
    }

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        evtNoticef("raft(%16llx) change conf failed %d", r->id, rv);
        return rv;
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_NOTFOUND;
        ErrMsgPrintf(r->errmsg, "no server has ID %llu", id);
        evtErrf("raft(%16llx) has no server id %llx failed", r->id, id, rv);
        goto err;
    }

    /* Check if we have already the desired role. */
    if (server->role == role) {
        const char *name;
        rv = RAFT_BADROLE;
        switch (role) {
            case RAFT_VOTER:
                name = "voter";
                break;
            case RAFT_STANDBY:
                name = "stand-by";
                break;
            case RAFT_SPARE:
                name = "spare";
                break;
            default:
                name = NULL;
                assert(0);
                break;
        }
        ErrMsgPrintf(r->errmsg, "server is already %s", name);
        evtWarnf("raft(%16llx) server %llx is already %s", r->id, server->id, name);
        goto err;
    }

    server_index = configurationIndexOf(&r->configuration, id);
    assert(server_index < r->configuration.n);

    last_index = logLastIndex(&r->log);

    req->cb = cb;

    assert(r->leader_state.change == NULL);
    r->leader_state.change = req;

    /* If we are not promoting to the voter role or if the log of this server is
     * already up-to-date, we can submit the configuration change
     * immediately. */
    if (role != RAFT_VOTER ||
        progressMatchIndex(r, server_index) == last_index) {
        int old_role = r->configuration.servers[server_index].role;
        r->configuration.servers[server_index].role = role;

        rv = clientChangeConfiguration(r, req, &r->configuration);
        if (rv != 0) {
            r->configuration.servers[server_index].role = old_role;
            evtErrf("raft(%16llx) change conf failed %d", r->id, rv);
            return rv;
        }

        return 0;
    }

    r->leader_state.promotee_id = server->id;

    progressUpdateMinMatch(r);

    /* Initialize the first catch-up round. */
    r->leader_state.round_number = 1;
    r->leader_state.round_index = last_index;
    r->leader_state.round_start = r->io->time(r->io);

    /* Immediately initiate an AppendEntries request. */
    rv = replicationProgress(r, server_index);
    if (rv != 0 && rv != RAFT_NOCONNECTION) {
        /* This error is not fatal. */
        tracef("failed to send append entries to server %llu: %s (%d)",
               server->id, raft_strerror(rv), rv);
        evtErrf("raft(%llx) replication progress failed %d", r->id, rv);
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int raft_remove(struct raft *r,
                struct raft_change *req,
                raft_id id,
                raft_change_cb cb)
{
    const struct raft_server *server;
    struct raft_configuration configuration;
    int rv;

    rv = membershipCanChangeConfiguration(r);
    if (rv != 0) {
        evtErrf("raft(%llx) change conf failed %d", r->id, rv);
        return rv;
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_BADID;
        evtErrf("raft(%llx) bad id %llx", r->id, id);
        goto err;
    }

    tracef("remove server: id %llu", id);

    /* Make a copy of the current configuration, and remove the given server
     * from it. */
    rv = configurationCopy(&r->configuration, &configuration);
    if (rv != 0) {
        evtErrf("raft(%llx) copy conf failed %d", r->id, rv);
        goto err;
    }

    rv = configurationRemove(&configuration, id);
    if (rv != 0) {
        evtErrf("raft(%llx) remove %llx from conf failed %d", r->id, id, rv);
        goto err_after_configuration_copy;
    }

    req->cb = cb;

    rv = clientChangeConfiguration(r, req, &configuration);
    if (rv != 0) {
        evtErrf("raft(%llx) change conf failed %d", r->id, rv);
        goto err_after_configuration_copy;
    }
    progressUpdateMinMatch(r);

    assert(r->leader_state.change == NULL);
    r->leader_state.change = req;

    return 0;

err_after_configuration_copy:
    raft_configuration_close(&configuration);

err:
    assert(rv != 0);
    return rv;
}

/* Find a suitable voting follower. */
static raft_id clientSelectTransferee(struct raft *r)
{
    const struct raft_server *transferee = NULL;
    unsigned i;

    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }
        transferee = server;
        if (progressIsUpToDate(r, i)) {
            break;
        }
    }

    if (transferee != NULL) {
        return transferee->id;
    }

    return 0;
}

int raft_transfer(struct raft *r,
                  struct raft_transfer *req,
                  raft_id id,
                  raft_transfer_cb cb)
{
    const struct raft_server *server;
    unsigned i;
    int rv;

    if (r->state != RAFT_LEADER || r->transfer != NULL) {
        rv = RAFT_NOTLEADER;
        ErrMsgFromCode(r->errmsg, rv);
        evtErrf("raft(%16llx) transfer %llx failed %d", r->id, id, rv);
        goto err;
    }

    if (id == 0) {
        id = clientSelectTransferee(r);
        if (id == 0) {
            rv = RAFT_NOTFOUND;
            ErrMsgPrintf(r->errmsg, "there's no other voting server");
            evtErrf("raft(%16llx) select transferee failed %d", r->id, rv);
            goto err;
        }
    }

    server = configurationGet(&r->configuration, id);
    if (server == NULL || server->id == r->id || server->role != RAFT_VOTER) {
        rv = RAFT_BADID;
        ErrMsgFromCode(r->errmsg, rv);
        evtErrf("raft(%16llx) get conf failed %d", r->id, rv);
        goto err;
    }

    /* If this follower is up-to-date, we can send it the TimeoutNow message
     * right away. */
    i = configurationIndexOf(&r->configuration, server->id);
    assert(i < r->configuration.n);

    membershipLeadershipTransferInit(r, req, id, cb);

    if (progressIsUpToDate(r, i)) {
        rv = membershipLeadershipTransferStart(r);
        if (rv != 0) {
            r->transfer = NULL;
            evtErrf("raft(%16llx) transfer to %llx failed %d", r->id, id, rv);
            goto err;
        }
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

#undef tracef
