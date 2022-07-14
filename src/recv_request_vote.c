#include "recv_request_vote.h"

#include "assert.h"
#include "election.h"
#include "recv.h"
#include "tracing.h"
#include "convert.h"
#include "event.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

static void requestVoteSendCb(struct raft_io_send *req, int status)
{
    (void)status;
    raft_free(req);
}
struct set_meta_req
{
    struct raft	*raft; /* Instance that has submitted the request */
    raft_term	term;
    raft_id		voted_for;
    struct raft_message message;
    struct raft_io_set_meta req;
};
static void respondToRequestVote(struct raft_io_set_meta *req, int status)
{
    struct set_meta_req *request = req->data;
    struct raft *r = request->raft;
    struct raft_io_send *reqs;
    int rv = 0;

    if (r->state == RAFT_UNAVAILABLE)
        goto err;
    r->io->state = RAFT_IO_AVAILABLE;
    if(status != 0) {
	evtErrf("raft %x set meta for rv %x %u failed %d", r->id,
		request->voted_for, request->term, status);
        convertToUnavailable(r);
        goto err;
    }

    r->current_term = request->term;
    r->voted_for = request->voted_for;

    if (r->state != RAFT_FOLLOWER) {
        /* Also convert to follower. */
        convertToFollower(r);
    }

    reqs = raft_malloc(sizeof *reqs);
    if (reqs == NULL) {
	evtErrf("raft %x malloc req failed", r->id);
        convertToUnavailable(r);
        goto err;
    }
    reqs->data = r;

    rv = r->io->send(r->io,
             reqs,
             &request->message,
             requestVoteSendCb);
    if (rv != 0) {
        raft_free(reqs);
    }
err:
    raft_free(request);
}

int recvUpdateMeta(struct raft *r,
        struct raft_message *message,
        raft_term	term,
        raft_id voted_for,
        raft_io_set_meta_cb cb);

int recvRequestVote(struct raft *r,
                    const raft_id id,
                    const struct raft_request_vote *args)
{
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_request_vote_result *result = &message.request_vote_result;
    raft_id voted_for = r->voted_for;
    bool has_leader;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);

    result->vote_granted = false;
    result->term = r->current_term;
    result->pre_vote = args->pre_vote;

    message.type = RAFT_IO_REQUEST_VOTE_RESULT;
    message.server_id = id;

    /* Reject the request if we have a leader.
         *
         * From Section 4.2.3:
         *
         *   [Removed] servers should not be able to disrupt a leader whose cluster
         *   is receiving heartbeats. [...] If a server receives a RequestVote
         *   request within the minimum election timeout of hearing from a current
         *   leader, it does not update its term or grant its vote
         *
         * From Section 4.2.3:
         *
         *   This change conflicts with the leadership transfer mechanism as
         *   described in Chapter 3, in which a server legitimately starts an
         *   election without waiting an election timeout. In that case, RequestVote
         *   messages should be processed by other servers even when they believe a
         *   current cluster leader exists. Those RequestVote requests can include a
         *   special flag to indicate this behavior ("I have permission to disrupt
         *   the leader - it told me to!").
         */

    has_leader = r->state == RAFT_LEADER ||
            (r->state == RAFT_FOLLOWER && r->follower_state.current_leader.id != 0 &&
            (r->io->time(r->io) - r->election_timer_start) < r->election_timeout);

    if (has_leader && !args->disrupt_leader) {
        tracef("local server has a leader -> reject ");
        goto reply;
    }

    recvCheckMatchingTerms(r, args->term, &match);
    if(match >= 0) {
        electionVote(r, args, &result->vote_granted);
        if (!args->pre_vote) {
            if(match > 0) {
                voted_for = 0;
                result->term = args->term;
            }

            if (result->vote_granted) {
                voted_for = args->candidate_id;
                /* Reset the election timer. */
                r->election_timer_start = r->io->time(r->io);
                ++match;
            }

            if(match > 0) {
                return recvUpdateMeta(r,
                                      &message,
                                      args->term,
                                      voted_for,
                                      respondToRequestVote);
            }
        }
    } else {
        tracef("local term is higher -> reject ");
    }

reply:
    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_NOMEM;
    }
    req->data = r;

    rv = r->io->send(r->io, req, &message, requestVoteSendCb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

#undef tracef
