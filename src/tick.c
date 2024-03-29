#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "membership.h"
#include "progress.h"
#include "replication.h"
#include "tracing.h"
#include "event.h"
#include "snapshot_sampler.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

/* Apply time-dependent rules for followers (Figure 3.1). */
static int tickFollower(struct raft *r)
{
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_FOLLOWER);

    server = configurationGet(&r->configuration, r->id);

    /* If we have been removed from the configuration, or maybe we didn't
     * receive one yet, just stay follower. */
    if (server == NULL) {
        evtWarnf("W-1528-072", "raft(%llx) have been remove from conf", r->id);
        return 0;
    }

    if ((r->ticks % r->tick_snapshot_frequency) == 0) {
        /* Try to apply and take snapshot*/
        rv = replicationApply(r);
        if (rv != 0) {
            evtErrf("E-1528-252", "raft(%llx) replication apply failed %d", r->id, rv);
            return rv;
        }

        replicationRemoveTrailing(r);
    }

    /* Check if we need to start an election.
     *
     * From Section 3.3:
     *
     *   If a follower receives no communication over a period of time called
     *   the election timeout, then it assumes there is no viable leader and
     *   begins an election to choose a new leader.
     *
     * Figure 3.1:
     *
     *   If election timeout elapses without receiving AppendEntries RPC from
     *   current leader or granting vote to candidate, convert to candidate.
     */
    if (!electionTimerExpired(r)) {
        return 0;
    }

    if (r->nr_appending_requests != 0) {
        evtNoticef("N-1528-058", "raft(%llx) has %u pending append requests", r->id,
            r->nr_appending_requests);
        return 0;
    }

    if (configurationIsVoter(&r->configuration, server, RAFT_GROUP_ANY)) {
        tracef("convert to candidate and start new election");
        evtInfof("I-1528-005", "raft(%llx) convert to candidate", r->id);
        rv = convertToCandidate(r, false /* disrupt leader */);
        if (rv != 0) {
            tracef("convert to candidate: %s", raft_strerror(rv));
            evtErrf("E-1528-253", "raft(%llx) convert to candidate failed %d", r->id, rv);
            return rv;
        }
    }

    return 0;
}

/* Apply time-dependent rules for candidates (Figure 3.1). */
static int tickCandidate(struct raft *r)
{
    assert(r != NULL);
    assert(r->state == RAFT_CANDIDATE);

    /* Check if we need to start an election.
     *
     * From Section 3.4:
     *
     *   The third possible outcome is that a candidate neither wins nor loses
     *   the election: if many followers become candidates at the same time,
     *   votes could be split so that no candidate obtains a majority. When this
     *   happens, each candidate will time out and start a new election by
     *   incrementing its term and initiating another round of RequestVote RPCs
     */
    if (electionTimerExpired(r)) {
        tracef("start new election");
        evtInfof("I-1528-006", "raft(%llx) start new election", r->id);
        r->candidate_state.in_pre_vote = r->pre_vote;
        r->candidate_state.disrupt_leader = false;
        return electionStart(r);
    }

    return 0;
}

static size_t contactQuorumForGroup(struct raft *r, int group)
{
    unsigned i;
    unsigned contacts = 0;
    assert(r->state == RAFT_LEADER);

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (!configurationIsVoter(&r->configuration, server, group))
            continue;
        bool recent_recv = progressGetRecentRecv(r, i);
        if (recent_recv || server->id == r->id) {
            contacts++;
        }
    }
    return contacts;
}

static bool checkContactQuorumForGroup(struct raft *r, int group)
{
    size_t n_voters = configurationVoterCount(&r->configuration, group);
    size_t contacts = contactQuorumForGroup(r, group);
    assert(r->state == RAFT_LEADER);

    if (r->quorum == RAFT_MAJORITY && contacts <= n_voters / 2)
	    return false;
    if (r->quorum == RAFT_FULL && contacts < n_voters)
	    return false;
    return true;
}

/* Return true if we received an AppendEntries RPC result from a majority of
 * voting servers since we became leaders or since the last time this function
 * was called.
 *
 * For each server the function checks the recent_recv flag of the associated
 * progress object, and resets the flag after the check. It returns true if a
 * majority of voting server had the flag set to true. */
static bool checkContactQuorum(struct raft *r)
{
    unsigned i;
    bool ret;
    assert(r->state == RAFT_LEADER);

    if (r->configuration.phase == RAFT_CONF_JOINT) {
        if (!checkContactQuorumForGroup(r, RAFT_GROUP_NEW))
            return false;
    }
    ret = checkContactQuorumForGroup(r, RAFT_GROUP_OLD);

    for (i = 0; i < r->configuration.n; i++) {
        (void)progressResetRecentRecv(r, i);
    }
    return ret;
}

static void checkChangeOnMatch(struct raft *r)
{
    unsigned i;
    raft_index index;
    struct raft_change *change;

    assert(r->state == RAFT_LEADER);
    change = r->leader_state.change;
    if (!change->cb_on_match || !r->enable_change_cb_on_match
        || r->leader_state.promotee_id)
        return;

    i = configurationIndexOf(&r->configuration, change->match_id);
    if (i == r->configuration.n)
        return;
    index = progressMatchIndex(r, i);
    if (index < change->index || change->index == 0)
        return;

    evtNoticef("N-1528-059", "raft(%llx) change on match, match_id %llx index %llu",
               r->id, change->match_id, change->index);
    r->leader_state.change = NULL;
    if (change != NULL && change->cb != NULL) {
        change->cb(change, 0);
    }
}

static void checkChangeTimeout(struct raft *r)
{
    struct raft_change *change;
    raft_time now = r->io->time(r->io);
    assert(r->state == RAFT_LEADER);

    change = r->leader_state.change;
    if (change->time
        + r->max_catch_up_rounds * r->max_catch_up_round_duration > now) {
        return;
    }

    r->leader_state.change = NULL;
    if (change != NULL && change->cb != NULL) {
        change->cb(change, RAFT_NOCONNECTION);
        evtErrf("E-1528-262", "raft(%llx) change timeout", r->id);
    }
}

bool tickCheckContactQuorum(struct raft *r)
{
    raft_time now = r->io->time(r->io);
    assert(r->state == RAFT_LEADER);

    if (now - r->election_timer_start >= (r->election_timeout - r->heartbeat_timeout)) {
        if (!checkContactQuorum(r)) {
            tracef("unable to contact majority of cluster -> step down");
            evtNoticef("N-1528-060", "raft(%llx) leader step down", r->id);
            if (r->stepdown_cb)
                r->stepdown_cb(r, RAFT_TICK_STEPDOWN);
            convertToFollower(r);
            return false;
        }
        r->election_timer_start = r->io->time(r->io);
    }

    return true;
}

/* Apply time-dependent rules for leaders (Figure 3.1). */
static int tickLeader(struct raft *r)
{
    int rv;
    raft_time now = r->io->time(r->io);
    assert(r->state == RAFT_LEADER);

    /* Check if we still can reach a majority of servers.
     *
     * From Section 6.2:
     *
     *   A leader in Raft steps down if an election timeout elapses without a
     *   successful round of heartbeats to a majority of its cluster; this
     *   allows clients to retry their requests with another server.
     */
    if (!tickCheckContactQuorum(r)) {
        tracef("tick check contact quorum failed.");
        return 0;
    }

    if ((r->ticks % r->tick_snapshot_frequency) == 0) {
        /* Try to apply and take snapshot*/
        rv = replicationApply(r);
        if (rv != 0) {
            evtErrf("E-1528-254", "raft(%llx) replication apply failed %d", r->id, rv);
        }

        if (r->state != RAFT_LEADER) {
            evtNoticef("N-1528-061", "raft(%llx) step down after replication apply", r->id);
            return 0;
        }

        if (r->sync_replication) {
            progressUpdateMinMatch(r);
        }

        replicationRemoveTrailing(r);
    }

    /* Possibly send heartbeats.
     *
     * From Figure 3.1:
     *
     *   Send empty AppendEntries RPC during idle periods to prevent election
     *   timeouts.
     */
    replicationHeartbeat(r);

    /* If a server is being promoted, increment the timer of the current
     * round or abort the promotion.
     *
     * From Section 4.2.1:
     *
     *   The algorithm waits a fixed number of rounds (such as 10). If the last
     *   round lasts less than an election timeout, then the leader adds the new
     *   server to the cluster, under the assumption that there are not enough
     *   unreplicated entries to create a significant availability
     *   gap. Otherwise, the leader aborts the configuration change with an
     *   error.
     */
    if (r->leader_state.promotee_id != 0) {
        raft_id id = r->leader_state.promotee_id;
        unsigned server_index;
        raft_time round_duration = now - r->leader_state.round_start;
        bool is_too_slow;
        bool is_unresponsive;

        /* If a promotion is in progress, we expect that our configuration
         * contains an entry for the server being promoted, and that the server
         * is not yet considered as voting. */
        server_index = configurationIndexOf(&r->configuration, id);
        assert(server_index < r->configuration.n);

        is_too_slow = (r->leader_state.round_number == r->max_catch_up_rounds &&
                       round_duration > r->election_timeout);
        is_unresponsive = round_duration > r->max_catch_up_round_duration;

        /* Abort the promotion if we are at the 10'th round and it's still
         * taking too long, or if the server is unresponsive. */
        if (is_too_slow || is_unresponsive) {
            struct raft_change *change;

            r->leader_state.promotee_id = 0;
            r->leader_state.round_index = 0;
            r->leader_state.round_number = 0;
            r->leader_state.round_start = 0;
            r->leader_state.remove_id = 0;

            change = r->leader_state.change;
            r->leader_state.change = NULL;
            if (change != NULL && change->cb != NULL) {
                change->cb(change, RAFT_NOCONNECTION);
            }
        }
    }

    if (r->leader_state.change) {
        checkChangeOnMatch(r);
    }

    if (r->leader_state.change) {
        checkChangeTimeout(r);
    }

    return 0;
}

static int tick(struct raft *r)
{
    int rv = -1;

    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);

    /* If we are not available, let's do nothing. */
    if (r->state == RAFT_UNAVAILABLE) {
        return 0;
    }

    ++r->ticks;
    switch (r->state) {
        case RAFT_FOLLOWER:
            rv = tickFollower(r);
            break;
        case RAFT_CANDIDATE:
            rv = tickCandidate(r);
            break;
        case RAFT_LEADER:
            rv = tickLeader(r);
            break;
    }

    return rv;
}

void tickCb(struct raft_io *io)
{
    struct raft *r;
    int rv;
    r = io->data;
    /* skip the tick if we are updating the meta */
    if (r->io->state != RAFT_IO_AVAILABLE) {
        return;
    }
    rv = tick(r);
    if (rv != 0) {
        evtErrf("E-1528-255", "raft %x tick failed %d", r->id, rv);
        convertToUnavailable(r);
        return;
    }
    /* For all states: if there is a leadership transfer request in progress,
     * check if it's expired. */
    if (r->transfer != NULL) {
        raft_time now = r->io->time(r->io);
        if (now - r->transfer->start >= r->election_timeout) {
            membershipLeadershipTransferClose(r);
        }
    }
}

#undef tracef
