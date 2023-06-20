#include "recv_append_entries_result.h"
#include "assert.h"
#include "configuration.h"
#include "tracing.h"
#include "recv.h"
#include "replication.h"
#include "progress.h"
#include "event.h"
#include "log.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void loggerLeadershipTransferCb(struct raft_transfer *req)
{
    raft_free(req);
}

static void loggerTransfer(struct raft *r, const raft_id id)
{
    int rv;
    unsigned i;
    struct raft_transfer *req;

    i = configurationIndexOf(&r->configuration, id);
    if (i == r->configuration.n) {
        tracef("server %llx is not part of configuration", id);
        return;
    }

    if (configurationServerRole(&r->configuration, id) != RAFT_VOTER) {
        tracef("server role is not voter");
        return;
    }

    if (progressMatchIndex(r, i) != logLastIndex(&r->log)) {
        tracef("server log is not catch up with leader");
        return;
    }

    tracef("other server have caught up, logger convert to follower");
    req = raft_malloc(sizeof(struct raft_transfer));
    if (req == NULL) {
        tracef("out of memory when malloc transfer request");
        return;
    }

    rv = raft_transfer(r, req, id, loggerLeadershipTransferCb);
    if (rv != 0) {
        tracef("transfer leader to %llx faild %d", id, rv);
    }
}

int recvAppendEntriesResult(struct raft *r,
                            const raft_id id,
                            const struct raft_append_entries_result *result)
{
    int match;
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(result != NULL);

    if (r->state != RAFT_LEADER) {
        tracef("local server is not leader -> ignore");
        return 0;
    }

    recvCheckMatchingTerms(r, result->term, &match);
    assert(match <= 0);

    if (match < 0) {
        tracef("local term is higher -> ignore ");
        return 0;
    }

    /* If we have stepped down, abort here.
     *
     * From Figure 3.1:
     *
     *   [Rules for Servers] All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     */
    if (match > 0) {
        assert(r->state == RAFT_FOLLOWER);
        return 0;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        tracef("unknown server -> ignore");
        evtWarnf("raft(%llx) ignore unknown server %llx", r->id, id);
        return 0;
    }

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server->id, result);
    if (rv != 0) {
        evtErrf("raft(%llx) replication update failed %d", r->id, rv);
        return rv;
    }

    if (r->role == RAFT_LOGGER) {
        loggerTransfer(r, id);
    }

    return 0;
}

#undef tracef
