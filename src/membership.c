#include "membership.h"

#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "progress.h"
#include "tracing.h"


int membershipCanChangeConfiguration(struct raft *r)
{
    int rv;

    if (r->state != RAFT_LEADER || r->transfer != NULL) {
        rv = RAFT_NOTLEADER;
        goto err;
    }

    if (r->configuration_uncommitted_index != 0) {
        rv = RAFT_CANTCHANGE;
        goto err;
    }

    if (r->leader_state.promotee_id != 0) {
        rv = RAFT_CANTCHANGE;
        goto err;
    }

    if (r->pgrep_id != RAFT_INVALID_ID) {
        rv = RAFT_CANTCHANGE;
        goto err;
    }

    /* In order to become leader at all we are supposed to have committed at
     * least the initial configuration at index 1. */
    assert(r->configuration_index > 0);

    /* The index of the last committed configuration can't be greater than the
     * last log index. */
    assert(logLastIndex(&r->log) >= r->configuration_index);

    /* No catch-up round should be in progress. */
    assert(r->leader_state.round_number == 0);
    assert(r->leader_state.round_index == 0);
    assert(r->leader_state.round_start == 0);

    return 0;

err:
    assert(rv != 0);
    ErrMsgFromCode(r->errmsg, rv);
    return rv;
}

int membershipUncommittedChange(struct raft *r,
                                const raft_index index,
                                const struct raft_entry *entry)
{
    struct raft_configuration configuration;
	const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_FOLLOWER);
    assert(entry != NULL);
    assert(entry->type == RAFT_CHANGE);

    raft_configuration_init(&configuration);

    rv = configurationDecode(&entry->buf, &configuration);
    if (rv != 0) {
        goto err;
    }

    raft_configuration_close(&r->configuration);

    r->configuration = configuration;
    r->configuration_uncommitted_index = index;

	ZSINFO(gzlog, "[raft][%d][%d][%s][conf_dump] set configuration_uncommitted_index = [%lld].",
		   rkey(r), r->state, __func__, r->configuration_uncommitted_index);

	/* Notify the upper module the role changed. */
	server = configurationGet(&r->configuration, r->id);
	if (server && server->role == RAFT_DYING && r->role_change_cb)
		r->role_change_cb(r, server);

	for (unsigned int i = 0; i < r->configuration.n; i++) {
		const struct raft_server *servert = &r->configuration.servers[i];
		ZSINFO(gzlog, "[raft][%d][%d][%s][conf_dump] i[%d] id[%lld] role[%d] pre_role[%d]",
			   rkey(r), r->state, __func__, i,
			   servert->id, servert->role, servert->pre_role);
	}

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int membershipRollback(struct raft *r)
{
    const struct raft_entry *entry;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_FOLLOWER);
    assert(r->configuration_uncommitted_index > 0);

    /* Fetch the last committed configuration entry. */
    assert(r->configuration_index != 0);

    entry = logGet(&r->log, r->configuration_index);

    assert(entry != NULL);

    /* Replace the current configuration with the last committed one. */
    raft_configuration_close(&r->configuration);
    raft_configuration_init(&r->configuration);

    rv = configurationDecode(&entry->buf, &r->configuration);
    if (rv != 0) {
        return rv;
    }

    r->configuration_uncommitted_index = 0;
    ZSINFO(gzlog, "[raft][%d][%d][%s][conf_dump] set configuration_uncommitted_index = [%lld].",
           rkey(r), r->state, __func__, r->configuration_uncommitted_index);

	for (unsigned int i = 0; i < r->configuration.n; i++) {
		const struct raft_server *servert = &r->configuration.servers[i];
		ZSINFO(gzlog, "[raft][%d][%d][%s][conf_dump] i[%d] id[%lld] role[%d] pre_role[%d]",
			   rkey(r), r->state, __func__, i,
			   servert->id, servert->role, servert->pre_role);
	}

    return 0;
}

void membershipLeadershipTransferInit(struct raft *r,
                                      struct raft_transfer *req,
                                      raft_id id,
                                      raft_transfer_cb cb)
{
    req->cb = cb;
    req->id = id;
    req->start = r->io->time(r->io);
    req->send.data = NULL;
    r->transfer = req;
}

int membershipLeadershipTransferStart(struct raft *r)
{
    const struct raft_server *server;
    struct raft_message message;
    int rv;
    assert(r->transfer->send.data == NULL);
    server = configurationGet(&r->configuration, r->transfer->id);
    assert(server != NULL);
    message.type = RAFT_IO_TIMEOUT_NOW;
    message.server_id = server->id;
    message.server_address = server->address;
    message.timeout_now.term = r->current_term;
    message.timeout_now.last_log_index = logLastIndex(&r->log);
    message.timeout_now.last_log_term = logLastTerm(&r->log);
    r->transfer->send.data = r;
    rv = r->io->send(r->io, &r->transfer->send, &message, NULL);
    if (rv != 0) {
        ErrMsgTransferf(r->io->errmsg, r->errmsg, "send timeout now to %llu",
                        server->id);
        return rv;
    }
    return 0;
}

void membershipLeadershipTransferClose(struct raft *r)
{
    struct raft_transfer *req = r->transfer;
    raft_transfer_cb cb = req->cb;
    r->transfer = NULL;
    if (cb != NULL) {
        cb(req);
    }
}
