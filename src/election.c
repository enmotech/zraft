#include "election.h"

#include "assert.h"
#include "configuration.h"
#include "heap.h"
#include "log.h"
#include "tracing.h"
#include "event.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

/* Common fields between follower and candidate state.
 *
 * The follower_state and candidate_state structs in raft.h must be kept
 * consistent with this definition. */
struct followerOrCandidateState
{
    unsigned randomized_election_timeout;
};

/* Return a pointer to either the follower or candidate state. */
struct followerOrCandidateState *getFollowerOrCandidateState(struct raft *r)
{
    struct followerOrCandidateState *state;
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    if (r->state == RAFT_FOLLOWER) {
        state = (struct followerOrCandidateState *)&r->follower_state;
    } else {
        state = (struct followerOrCandidateState *)&r->candidate_state;
    }
    return state;
}

void electionResetTimer(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    unsigned timeout = (unsigned)r->io->random(r->io, (int)r->election_timeout,
                                               2 * (int)r->election_timeout);
    assert(timeout >= r->election_timeout);
    assert(timeout <= r->election_timeout * 2);
    state->randomized_election_timeout = timeout;
    r->election_timer_start = r->io->time(r->io);
}

bool electionTimerExpired(struct raft *r)
{
    struct followerOrCandidateState *state = getFollowerOrCandidateState(r);
    raft_time now = r->io->time(r->io);
    return now - r->election_timer_start >= state->randomized_election_timeout;
}

static void sendRequestVoteCb(struct raft_io_send *send, int status)
{
    (void)status;
    HeapFree(send);
}

/* Send a RequestVote RPC to the given server. */
static int electionSend(struct raft *r, const struct raft_server *server)
{
    struct raft_message message;
    struct raft_io_send *send;
    raft_term term;
    int rv;
    assert(server->id != r->id);
    assert(server->id != 0);

    /* If we are in the pre-vote phase, we indicate our future term in the
     * request. */
    term = r->current_term;
    if (r->candidate_state.in_pre_vote) {
        term++;
    }

    message.type = RAFT_IO_REQUEST_VOTE;
    message.request_vote.term = term;
    message.request_vote.candidate_id = r->id;
    message.request_vote.last_log_index = logLastIndex(&r->log);
    message.request_vote.last_log_term = logLastTerm(&r->log);
    message.request_vote.disrupt_leader = r->candidate_state.disrupt_leader;
    message.request_vote.pre_vote = r->candidate_state.in_pre_vote;
    message.server_id = server->id;

    send = HeapMalloc(sizeof *send);
    if (send == NULL) {
        evtErrf("raft(%16llx) heap malloc failed", r->id);
        return RAFT_NOMEM;
    }

    send->data = r;

    rv = r->io->send(r->io, send, &message, sendRequestVoteCb);
    if (rv != 0) {
        if (rv != RAFT_NOCONNECTION)
            evtErrf("raft(%16llx) election send failed %d", r->id, rv);
        HeapFree(send);
        return rv;
    }

    return 0;
}
#include "convert.h"
struct set_meta_election
{
    struct raft *raft; /* Instance that has submitted the request */
    raft_term	term;
    raft_id		voted_for;
    struct raft_io_set_meta req;
};

static void electionSetMetaCb(struct raft_io_set_meta *req, int status)
{
    struct set_meta_election *request = req->data;
    struct raft *r = request->raft;
    size_t n_voters;
    size_t voting_index;
    size_t i;
    int rv;

    if (r->state == RAFT_UNAVAILABLE)
        goto err;
    assert(r->state == RAFT_CANDIDATE);
    r->io->state = RAFT_IO_AVAILABLE;
    if(status != 0) {
	evtErrf("raft(%16llx) set meta failed %d", r->id, status);
        convertToUnavailable(r);
        goto err;
    }

    r->current_term = request->term;
    r->voted_for = request->voted_for;

    /* Reset election timer. */
    electionResetTimer(r);

    assert(r->candidate_state.votes != NULL);

    n_voters = configurationVoterCount(&r->configuration);
    voting_index = configurationIndexOfVoter(&r->configuration, r->id);
    /* Initialize the votes array and send vote requests. */
    for (i = 0; i < n_voters; i++) {
        if (i == voting_index) {
            r->candidate_state.votes[i] = true; /* We vote for ourselves */
        } else {
            r->candidate_state.votes[i] = false;
        }
    }
    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }
        rv = electionSend(r, server);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            tracef("failed to send vote request to server %llu: %s", server->id,
                   raft_strerror(rv));
            if (rv != RAFT_NOCONNECTION)
                evtErrf("send vote to server %llx failed %d", server->id, rv);
        }
    }

err:
    raft_free(request);
}

static int electionUpdateMeta(struct raft *r)
{
    int rv;
    struct set_meta_election *request;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        evtErrf("raft(%16llx) malloc failed %d", r->id, rv);
        goto err2;
    }

    request->raft = r;
    request->term = r->current_term + 1;
    request->voted_for = r->id;
    request->req.data = request;


    r->io->state = RAFT_IO_BUSY;
    rv = r->io->set_meta(r->io,
                 &request->req,
                 request->term,
                 request->voted_for,
                 electionSetMetaCb);
    if (rv != 0) {
        evtErrf("raft(%16llx) set meta failed %d", r->id, rv);
        goto err1;
    }

    return 0;

err1:
    raft_free(request);
err2:
    return rv;
}
int electionStart(struct raft *r)
{
    size_t n_voters;
    size_t voting_index;
    size_t i;
    int rv;
    assert(r->state == RAFT_CANDIDATE);

    n_voters = configurationVoterCount(&r->configuration);
    voting_index = configurationIndexOfVoter(&r->configuration, r->id);

    /* This function should not be invoked if we are not a voting server, hence
     * voting_index must be lower than the number of servers in the
     * configuration (meaning that we are a voting server). */
    assert(voting_index < r->configuration.n);
    /* Coherence check that configurationVoterCount and configurationIndexOfVoter
     * have returned something that makes sense. */
    assert(n_voters <= r->configuration.n);
    assert(voting_index < n_voters);

    /* During pre-vote we don't actually increment term or persist vote, however
     * we reset any vote that we previously granted since we have timed out and
     * that vote is no longer valid. */
    if (!r->candidate_state.in_pre_vote) {
        return electionUpdateMeta(r);
    }
    /* Reset election timer. */
    electionResetTimer(r);
    assert(r->candidate_state.votes != NULL);
    /* Initialize the votes array and send vote requests. */
    for (i = 0; i < n_voters; i++) {
        if (i == voting_index) {
            r->candidate_state.votes[i] = true; /* We vote for ourselves */
        } else {
            r->candidate_state.votes[i] = false;
        }
    }
    for (i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id || server->role != RAFT_VOTER) {
            continue;
        }
        rv = electionSend(r, server);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            tracef("failed to send vote request to server %llu: %s", server->id,
                   raft_strerror(rv));
	    if (rv != RAFT_NOCONNECTION)
                evtErrf("send vote to server %llx failed %d", server->id, rv);
        }
    }

    return 0;
}

void electionVote(struct raft *r,
                 const struct raft_request_vote *args,
                 bool *granted)
{
    const struct raft_server *local_server;
    raft_index local_last_index;
    raft_term local_last_term;
    bool is_transferee; /* Requester is the target of a leadership transfer */

    assert(r != NULL);
    assert(args != NULL);
    assert(granted != NULL);

    local_server = configurationGet(&r->configuration, r->id);

    *granted = false;

    if (local_server == NULL || local_server->role != RAFT_VOTER) {
        tracef("local server is not voting -> not granting vote");
        return;
    }

    is_transferee =
        r->transfer != NULL && r->transfer->id == args->candidate_id;
    if (!is_transferee) {
        if (r->current_term == args->term) {
            if (r->voted_for != 0 && r->voted_for != args->candidate_id) {
                tracef("local server already voted -> not granting vote");
                return;
            }
        }
    }

    local_last_index = logLastIndex(&r->log);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (local_last_index == 0) {
        tracef("local log is empty -> granting vote");
        goto grant_vote;
    }

    local_last_term = logLastTerm(&r->log);

    if (args->last_log_term < local_last_term) {
        /* The requesting server has last entry's log term lower than ours. */
        tracef(
            "local last entry %llu has term %llu higher than %llu -> not "
            "granting",
            local_last_index, local_last_term, args->last_log_term);
        return;
    }

    if (args->last_log_term > local_last_term) {
        /* The requesting server has a more up-to-date log. */
        tracef(
            "remote last entry %llu has term %llu higher than %llu -> "
            "granting vote",
            args->last_log_index, args->last_log_term, local_last_term);
        goto grant_vote;
    }

    /* The term of the last log entry is the same, so let's compare the length
     * of the log. */
    assert(args->last_log_term == local_last_term);

    if (local_last_index <= args->last_log_index) {
        /* Our log is shorter or equal to the one of the requester. */
        tracef("remote log equal or longer than local -> granting vote");
        goto grant_vote;
    }

    tracef("remote log shorter than local -> not granting vote");

    return;
grant_vote:
    *granted = true;

}

bool electionTally(struct raft *r, size_t voter_index)
{
    size_t n_voters = configurationVoterCount(&r->configuration);
    size_t votes = 0;
    size_t i;
    size_t half = n_voters / 2;

    assert(r->state == RAFT_CANDIDATE);
    assert(r->candidate_state.votes != NULL);

    r->candidate_state.votes[voter_index] = true;

    for (i = 0; i < n_voters; i++) {
        if (r->candidate_state.votes[i]) {
            votes++;
        }
    }

    if (r->quorum == RAFT_MAJORITY)
	    return votes >= half + 1;

    assert(r->quorum == RAFT_FULL);
    return votes >= n_voters;
}

#undef tracef
