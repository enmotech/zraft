#include "recv_timeout_now.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "log.h"
#include "recv.h"
#include "tracing.h"
#include "event.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

int recvTimeoutNow(struct raft *r,
                   const raft_id id,
                   const struct raft_timeout_now *args)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);

    /* Ignore the request if we are not voters. */
    local_server = configurationGet(&r->configuration, r->id);
    if (local_server == NULL || !configurationIsVoter(&r->configuration,
        local_server, RAFT_GROUP_ANY)) {
        return 0;
    }

    /* Ignore the request if we are not follower, or we have different
     * leader. */
    if (r->state != RAFT_FOLLOWER ||
        r->follower_state.current_leader.id != id) {
        return 0;
    }

    if (r->nr_appending_requests != 0) {
        evtNoticef("N-1528-037", "raft(%llx) has %u pending append requests", r->id,
            r->nr_appending_requests);
        return 0;
    }

    /* Possibly update our term. Ignore the request if it turns out we have a
     * higher term. */
    recvCheckMatchingTerms(r, args->term, &match);
    assert(match <= 0);
    if (match < 0) {
        return 0;
    }

    /* Ignore the request if we our log is not up-to-date. */
    local_last_index = logLastIndex(&r->log);
    local_last_term = logLastTerm(&r->log);
    if (local_last_index < args->last_log_index ||
        local_last_term < args->last_log_term) {
        return 0;
    }

    /* Convert to candidate and start a new election. */
    rv = convertToCandidate(r, true /* disrupt leader */);
    if (rv != 0) {
        evtErrf("E-1528-169", "raft(%llx) convert to candidate failed %d", rv);
        return rv;
    }

    return 0;
}

#undef tracef
