#include "recv_append_entries_result.h"
#include "assert.h"
#include "configuration.h"
#include "tracing.h"
#include "recv.h"
#include "replication.h"
#include "event.h"
#include "log.h"
#include "convert.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void loggerLeadershipTransferCb(struct raft_transfer *req)
{
    if (req != NULL)
        raft_free(req);
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

    //如果logger收到了voter的AER且result->last_log_index >= logLastIndex(&r->log), logger就让权给该voter
    if (getRaftRole(r, r->id) == RAFT_LOGGER && getRaftRole(r, id) == RAFT_VOTER && result->last_log_index >= logLastIndex(&r->log))
    {
        tracef("other server have caught up, logger convert to follower");
        // convertToFollower(r);
        // r->follower_state.randomized_election_timeout *= 2;
        struct raft_transfer *req = raft_malloc(sizeof(struct raft_transfer));
        rv = raft_transfer(r, req, id, loggerLeadershipTransferCb);
        return rv;
    }

    return 0;
}

#undef tracef
