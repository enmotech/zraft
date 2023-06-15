#include "convert.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "request.h"
#include "replication.h"
#include "tracing.h"
#include "event.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void convertSetState(struct raft *r, unsigned short new_state)
{
    /* Check that the transition is legal, see Figure 3.3. Note that with
     * respect to the paper we have an additional "unavailable" state, which is
     * the initial or final state. */
    assert((r->state == RAFT_UNAVAILABLE && new_state == RAFT_FOLLOWER) ||
           (r->state == RAFT_FOLLOWER && new_state == RAFT_CANDIDATE) ||
           (r->state == RAFT_CANDIDATE && new_state == RAFT_FOLLOWER) ||
           (r->state == RAFT_CANDIDATE && new_state == RAFT_LEADER) ||
           (r->state == RAFT_LEADER && new_state == RAFT_FOLLOWER) ||
           (r->state == RAFT_FOLLOWER && new_state == RAFT_UNAVAILABLE) ||
           (r->state == RAFT_CANDIDATE && new_state == RAFT_UNAVAILABLE) ||
           (r->state == RAFT_LEADER && new_state == RAFT_UNAVAILABLE));
    r->state = new_state;
}

/* Clear follower state. */
static void convertClearFollower(struct raft *r)
{
    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.snapshot_index = 0;
    r->follower_state.current_leader.trailing = 0;
    r->follower_aux.match_leader = false;
}

/* Clear candidate state. */
static void convertClearCandidate(struct raft *r)
{
    if (r->candidate_state.votes != NULL) {
        raft_free(r->candidate_state.votes);
        r->candidate_state.votes = NULL;
    }
}

static void convertFailApply(struct raft_apply *req)
{
    if (req != NULL && req->cb != NULL) {
        req->cb(req, RAFT_LEADERSHIPLOST, NULL);
    }
}

static void convertFailBarrier(struct raft_barrier *req)
{
    if (req != NULL && req->cb != NULL) {
        req->cb(req, RAFT_LEADERSHIPLOST);
    }
}

static void convertFailChange(struct raft_change *req)
{
    if (req != NULL && req->cb != NULL) {
        req->cb(req, RAFT_LEADERSHIPLOST);
    }
}

/* Clear leader state. */
static void convertClearLeader(struct raft *r)
{
    if (r->leader_state.progress != NULL) {
        raft_free(r->leader_state.progress);
        r->leader_state.progress = NULL;
    }

    /* Fail all outstanding requests */
    while (requestRegNumRequests(&r->leader_state.reg)) {
        struct request *req = requestRegDequeue(&r->leader_state.reg);
	if (req == NULL)
		continue;
        assert(req->type == RAFT_COMMAND || req->type == RAFT_BARRIER);
        switch (req->type) {
            case RAFT_COMMAND:
                convertFailApply((struct raft_apply *)req);
                break;
            case RAFT_BARRIER:
                convertFailBarrier((struct raft_barrier *)req);
                break;
        };
    }
    requestRegClose(&r->leader_state.reg);

    /* Fail any promote request that is still outstanding because the server is
     * still catching up and no entry was submitted. */
    if (r->leader_state.change != NULL) {
        convertFailChange(r->leader_state.change);
        r->leader_state.change = NULL;
    }
}

/* Clear the current state */
static void convertClear(struct raft *r)
{
    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);
    switch (r->state) {
        case RAFT_FOLLOWER:
            convertClearFollower(r);
            break;
        case RAFT_CANDIDATE:
            convertClearCandidate(r);
            break;
        case RAFT_LEADER:
            convertClearLeader(r);
            break;
    }
}

void convertToFollower(struct raft *r)
{
    if (r->state == RAFT_LEADER) {
        evtNoticef("raft(%llx) leader step down", r->id);
    }

    convertClear(r);
    convertSetState(r, RAFT_FOLLOWER);

    /* Reset election timer. */
    electionResetTimer(r);

    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.snapshot_index = 0;
    r->follower_state.current_leader.trailing = r->snapshot.trailing;
    if (r->state_change_cb)
		r->state_change_cb(r, RAFT_FOLLOWER);
}

int convertToCandidate(struct raft *r, bool disrupt_leader)
{
    const struct raft_server *server;
    size_t n_voters = configurationVoterCount(&r->configuration,
                                              RAFT_GROUP_ANY);
    int rv;

    (void)server; /* Only used for assertions. */

    convertClear(r);
    convertSetState(r, RAFT_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voters * sizeof(bool));
    if (r->candidate_state.votes == NULL) {
        evtErrf("%s", "malloc");
        return RAFT_NOMEM;
    }
    r->candidate_state.disrupt_leader = disrupt_leader;
    r->candidate_state.in_pre_vote = disrupt_leader ? false : r->pre_vote;

    /* Fast-forward to leader if we're the only voting server in the
     * configuration. */
    server = configurationGet(&r->configuration, r->id);
    assert(server != NULL);
    assert(server->role == RAFT_VOTER || server->role_new == RAFT_VOTER
        || server->role == RAFT_LOGGER || server->role_new == RAFT_LOGGER);

    if (n_voters == 1) {
        tracef("self elect and convert to leader");
        evtInfof("raft(%llx) self elect and convert to leader", r->id);
        rv =  convertToLeader(r);
        if (rv != 0) {
            evtErrf("raft(%llx) convert to leader failed %d", r->id, rv);
            return rv;
        }
        /* Check if we can commit some new entries. */
        replicationQuorum(r, r->last_stored);

        rv = replicationApply(r);
        if (rv != 0) {
            /* TODO: just log the error? */
	    evtErrf("raft(%llx) replication apply failed %d", r->id, rv);
            convertToUnavailable(r);
        }
        return rv;
    }

    /* Start a new election round */
    rv = electionStart(r);
    if (rv != 0) {
        r->state = RAFT_FOLLOWER;
        raft_free(r->candidate_state.votes);
        evtErrf("raft(%llx) election start failed", r->id, rv);
        return rv;
    }
    if (r->state_change_cb)
		r->state_change_cb(r, RAFT_CANDIDATE);
    return 0;
}

int convertToLeader(struct raft *r)
{
    int rv;

    convertClear(r);
    convertSetState(r, RAFT_LEADER);

    /* Reset timers */
    r->election_timer_start = r->io->time(r->io);

    /* ReInit request registry */
    requestRegInit(&r->leader_state.reg);

    /* Allocate and initialize the progress array. */
    rv = progressBuildArray(r);
    if (rv != 0) {
        evtErrf("raft(%llx) build array failed %d", r->id, rv);
        return rv;
    }
    if (r->enable_free_trailing) {
        logResetUnFreedIndex(&r->log);
        evtInfof("raft(%llx) reset unfreed index %llu", r->id,
            logUnFreedIndex(&r->log));
    }

    r->leader_state.change = NULL;

    /* Reset promotion state. */
    r->leader_state.promotee_id = 0;
    r->leader_state.round_number = 0;
    r->leader_state.round_index = 0;
    r->leader_state.round_start = 0;
    r->leader_state.remove_id = 0;
    r->leader_state.removed_from_cluster = false;

    if (r->state_change_cb)
		r->state_change_cb(r, RAFT_LEADER);
    return 0;
}

void convertToUnavailable(struct raft *r)
{
    /* Abort any pending leadership transfer request. */
    if (r->transfer != NULL) {
        membershipLeadershipTransferClose(r);
    }
    convertClear(r);
    convertSetState(r, RAFT_UNAVAILABLE);
    if (r->state_change_cb)
		r->state_change_cb(r, RAFT_UNAVAILABLE);
}

#undef tracef
