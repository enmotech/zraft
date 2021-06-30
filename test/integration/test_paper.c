#include "../../src/configuration.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../../src/election.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
	FIXTURE_CLUSTER;
};
#ifdef CLUSTER_N
#undef CLUSTER_N
#define CLUSTER_N 3
#endif

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	SETUP_CLUSTER(CLUSTER_N);
	CLUSTER_BOOTSTRAP;
	for (i = 0; i < CLUSTER_N; i++) {
		struct raft *raft = CLUSTER_RAFT(i);
		raft->data = f;
	}
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	TEAR_DOWN_CLUSTER;
	free(f);
}
/* Assert that the I'th server has the given current term. */
#define ASSERT_TERM(I, TERM)                             \
    {                                                    \
        struct raft *raft_ = CLUSTER_RAFT(I);            \
        munit_assert_int(raft_->current_term, ==, TERM); \
    }
/* Assert that the I'th server is in follower state. */
#define ASSERT_FOLLOWER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_FOLLOWER)

/* Assert that the I'th server is in candidate state. */
#define ASSERT_CANDIDATE(I) \
    munit_assert_int(CLUSTER_STATE(I), ==, RAFT_CANDIDATE)

/* Assert that the I'th server is in leader state. */
#define ASSERT_LEADER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_LEADER)

SUITE(paper_test)
#include <unistd.h>
/* follower update it's term when receive higher term AE. */
TEST(paper_test, followerUpdateTermFromAE, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;

	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//let server k disconnect from cluster
	CLUSTER_SATURATE_BOTHWAYS(k,j);
	CLUSTER_SATURATE_BOTHWAYS(k,i);

	//let server j be new leader and add term
	CLUSTER_DEPOSE;
	CLUSTER_ELECT(j);
	ASSERT_LEADER(j);
	ASSERT_FOLLOWER(i);
	raft_term t = CLUSTER_TERM(j);
	//CLUSTER_STEP_UNTIL_TERM_IS(j, t+1, 1000);
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(j, &entry1);

	CLUSTER_DESATURATE_BOTHWAYS(j,k);
	CLUSTER_STEP_UNTIL_DELIVERED(j, k, 100);
	ASSERT_TERM(k,t);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}

TEST(paper_test, candidateUpdateTermFromAE, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	//let server k isolate from cluster
	CLUSTER_SATURATE_BOTHWAYS(k,j);
	CLUSTER_SATURATE_BOTHWAYS(k,i);
	CLUSTER_STEP_UNTIL_STATE_IS(k, RAFT_CANDIDATE, 2000);

	j=CLUSTER_LEADER;
	raft_term t = CLUSTER_TERM(j);
//	CLUSTER_STEP_UNTIL_TERM_IS(j, t+1, 2000);
//	unsigned m = CLUSTER_LEADER;
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(j, &entry1);
	CLUSTER_DESATURATE_BOTHWAYS(k,j);
	CLUSTER_STEP_UNTIL_DELIVERED(j, k, 100);
	ASSERT_TERM(k,t);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}

TEST(paper_test, leaderUpdateTermFromAE, setUp, tearDown, 0, NULL)
{
	//elect server i as leader
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	raft_term t = CLUSTER_TERM(i);

	//let server i disconnect from cluster
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);

	//let server j be another leader
	CLUSTER_DEPOSE;
	CLUSTER_ELECT(j);
	ASSERT_LEADER(j);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(k);

	//server add entry
	t = CLUSTER_TERM(j);
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(j, &entry1);

	//restore network of server i and deliver entry to i
	CLUSTER_DESATURATE_BOTHWAYS(i,j);
	CLUSTER_STEP_UNTIL_DELIVERED(j, i, 100);
	ASSERT_TERM(i,t);
	ASSERT_FOLLOWER(i);

	return MUNIT_OK;
}

TEST(paper_test, rejectStaleTermAE, setUp, tearDown, 0, NULL)
{
	//elect server i as leader
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);

	//let server i disconnect from cluster
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);

	//let server j be another leader
	CLUSTER_ELECT(j);
	ASSERT_LEADER(j);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(k);
	raft_term t = CLUSTER_TERM(i);
	raft_term t1 = CLUSTER_TERM(j);
	munit_assert_llong(t1, >, t);

	//server add entry
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(i, &entry1);

	//restore network of server i and deliver entry to i
	CLUSTER_DESATURATE_BOTHWAYS(i,j);
	CLUSTER_STEP_UNTIL_DELIVERED(i, j, 100);
	ASSERT_TERM(j,t1);
	ASSERT_LEADER(j);

	return MUNIT_OK;
}

//test each server start with state of follower
TEST(paper_test, startAsFollower, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i = 0, j = 1, k = 2;
	CLUSTER_START;

	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}

//test leader broadcast heartbeat
TEST(paper_test, leaderBcastBeat, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i = 0, j = 1, k = 2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//todo

	return MUNIT_OK;
}

//follower start a new election, test vote for itself, state hold candidate, add term
TEST(paper_test, followerStartElection, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i = 0, j = 1, k = 2;
	CLUSTER_START;
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	raft_term t = CLUSTER_TERM(i);

	raft_fixture_set_election_timeout_min(&f->cluster, k);
	CLUSTER_STEP_UNTIL_STATE_IS(k, RAFT_CANDIDATE, 2000);
	unsigned vote_for = CLUSTER_VOTED_FOR(k);
	munit_assert_int32(vote_for--, ==, k);
	ASSERT_CANDIDATE(k);
	raft_term t1 = CLUSTER_TERM(k);
	munit_assert_llong(t1, ==, t+1);
	return MUNIT_OK;
}

//candidate start a new election, test vote for itself, state change to candidate, add term
TEST(paper_test, candidateStartElection, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i = 0, j = 1, k = 2;
	CLUSTER_START;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);

	ASSERT_FOLLOWER(i);
	raft_fixture_step_until_state_is(&f->cluster, i, RAFT_CANDIDATE, 2000);
	ASSERT_CANDIDATE(i);

	raft_term t = CLUSTER_TERM(i);
	struct raft *r = CLUSTER_RAFT(i);
	electionStart(r);

	unsigned vote_for = CLUSTER_VOTED_FOR(i);
	munit_assert_int32(vote_for, ==, i);
	ASSERT_CANDIDATE(i);
	raft_term t1 = CLUSTER_TERM(i);
	munit_assert_llong(t1, ==, t+1);
	return MUNIT_OK;
}

//test follower vote, basis on first come first serve
TEST(paper_test, followerVote, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i = 0, j = 1, k = 2;
	CLUSTER_START;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	ASSERT_FOLLOWER(i);
	raft_term t = CLUSTER_TERM(i);
	struct raft *r = CLUSTER_RAFT(i);
	struct raft_request_vote req = {
		.candidate_id = j,
		.last_log_term = 0,
		.term = t+1
	};

	bool grant = false;
	electionVote(r, &req, &grant);
	munit_assert(grant == true);

	//vote for the same candidate again
	electionVote(r, &req, &grant);
	munit_assert(grant == true);

	//reject this rv, cause already vote for server k
	req.candidate_id = k;
	electionVote(r, &req, &grant);
	munit_assert(grant == false);
	return MUNIT_OK;
}

//test candidate recv a AE which term bigger than itself,
//then it change state to follower and update it's term
TEST(paper_test, candidateFallBack, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	//let server k disconnect from cluster
	CLUSTER_SATURATE_BOTHWAYS(k,j);
	CLUSTER_SATURATE_BOTHWAYS(k,i);

	struct raft *r = CLUSTER_RAFT(k);
	electionStart(r);
	ASSERT_CANDIDATE(k);

	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_CANDIDATE(k);

	raft_term t = CLUSTER_TERM(i);
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(i, &entry1);
	CLUSTER_DESATURATE_BOTHWAYS(k,i);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);
	ASSERT_TERM(k,t);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}
