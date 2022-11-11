#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "queue.h"

int raft_state(struct raft *r)
{
    return r->state;
}

void raft_leader(struct raft *r, raft_id *id)
{
    raft_time tm = r->io->time(r->io);

    switch (r->state) {
        case RAFT_UNAVAILABLE:
        case RAFT_CANDIDATE:
            *id = 0;
            return;
        case RAFT_FOLLOWER:
            if (tm > r->election_timer_start + r->election_timeout * 2) {
                *id = 0;
                return;
            }
            *id = r->follower_state.current_leader.id;
            return;
        case RAFT_LEADER:
            if (r->transfer != NULL) {
                *id = 0;
                return;
            }
            *id = r->id;
            return;
    }
}

raft_index raft_last_index(struct raft *r)
{
    return logLastIndex(&r->log);
}

raft_index raft_last_applied(struct raft *r)
{
    return r->last_applied;
}
