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

/* Assert that the fixture time matches the given value */
#define ASSERT_TIME(TIME) munit_assert_int(CLUSTER_TIME, ==, TIME)
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

	return MUNIT_OK;
}

TEST(paper_test, candidateUpdateTermFromAE, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_ELECT(k);
	ASSERT_LEADER(k);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);

	//let server k isolate from cluster
	CLUSTER_SATURATE_BOTHWAYS(k,j);
	CLUSTER_SATURATE_BOTHWAYS(k,i);
	CLUSTER_STEP_UNTIL_STATE_IS(k, RAFT_FOLLOWER, 2000);
	CLUSTER_STEP_UNTIL_STATE_IS(k, RAFT_CANDIDATE, 2000);
	raft_term t1 = CLUSTER_TERM(k);

	CLUSTER_STEP_UNTIL_STATE_IS(i, RAFT_LEADER, 2000);
	raft_term t2 = CLUSTER_TERM(i);
	munit_assert_llong(t1, <, t2);

	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t2;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(i, &entry1);
	CLUSTER_DESATURATE_BOTHWAYS(k,i);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);
	ASSERT_TERM(k,t2);

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
	ASSERT_FOLLOWER(k);

	//set a big election_timeout to avoid leader i step down
	raft_set_election_timeout(CLUSTER_RAFT(i), 100000);
	//let server i disconnect from cluster
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_STEP_UNTIL_STATE_IS(j, RAFT_LEADER, 20000);
	ASSERT_FOLLOWER(k);
	ASSERT_LEADER(i);

	raft_term t1 = CLUSTER_TERM(i);
	raft_term t2 = CLUSTER_TERM(j);
	munit_assert_llong(t1, <, t2);

	//server add entry
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t2;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(j, &entry1);

	//restore network of server i and deliver entry to i
	CLUSTER_DESATURATE_BOTHWAYS(i,j);
	raft_set_election_timeout(CLUSTER_RAFT(i), 1000);
	CLUSTER_STEP_UNTIL_DELIVERED(j, i, 100);
	ASSERT_TERM(i,t2);

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
	ASSERT_FOLLOWER(k);
	raft_set_election_timeout(CLUSTER_RAFT(i), 100000);
	//let server i disconnect from cluster
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_STEP_UNTIL_STATE_IS(j, RAFT_LEADER, 20000);
	ASSERT_FOLLOWER(k);
	ASSERT_LEADER(i);

	raft_term t1 = CLUSTER_TERM(i);
	raft_term t2 = CLUSTER_TERM(j);
	munit_assert_llong(t1, <, t2);

	//server add entry
	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t1;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(i, &entry1);

	//restore network of server i and deliver entry to i
	CLUSTER_DESATURATE_BOTHWAYS(i,j);
	raft_set_election_timeout(CLUSTER_RAFT(i), 1000);
	CLUSTER_STEP_UNTIL_DELIVERED(i, j, 100);

	//make sure server j still be the leader, indicate it reject the
	//lower term AE from server i
	ASSERT_TERM(j,t2);
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
	raft_term t = CLUSTER_TERM(i);

	//without any AE, after a max election_timeout, test the cluster still
	//the same leader and the same term
	CLUSTER_STEP_UNTIL_ELAPSED(2000);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	munit_assert_llong(t, =, CLUSTER_TERM(i));
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
	munit_assert_int32(vote_for-1, ==, k);
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
	munit_assert_int32(vote_for-1, ==, i);
	ASSERT_CANDIDATE(i);
	raft_term t1 = CLUSTER_TERM(i);
	munit_assert_llong(t1, ==, t+1);
	return MUNIT_OK;
}

//test follower vote, basis on first come first serve
TEST(paper_test, followerVote, setUp, tearDown, 0, NULL) {
    struct fixture *f = data;
	unsigned i = 0, j = 1, k = 2;

	/* drop all the msg between i and j */
	CLUSTER_START;
	CLUSTER_SET_NETWORK_LATENCY(i,200);
	CLUSTER_SET_NETWORK_LATENCY(i,200);
	raft_fixture_set_randomized_election_timeout(&f->cluster, i, 400);
	raft_fixture_set_randomized_election_timeout(&f->cluster, j, 500);
	raft_fixture_set_randomized_election_timeout(&f->cluster, k, 2000);
	raft_set_election_timeout(CLUSTER_RAFT(i), 400);
	raft_set_election_timeout(CLUSTER_RAFT(j), 500);
	raft_set_election_timeout(CLUSTER_RAFT(k), 2000);

	CLUSTER_STEP_UNTIL_STATE_IS(i, RAFT_CANDIDATE,2000);
	//server 0 election timeout, then be the first candidate
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TIME(400);
	CLUSTER_STEP_UNTIL_ELAPSED(100);
	//all the RV send by i, still in the network
	munit_assert_int(CLUSTER_N_SEND(i, RAFT_IO_REQUEST_VOTE), == ,2);
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_REQUEST_VOTE), == ,0);
	munit_assert_int(CLUSTER_N_RECV(k, RAFT_IO_REQUEST_VOTE), == ,0);
	ASSERT_TIME(400);
	ASSERT_CANDIDATE(j);
	/* Server 0 tick send RV */
	CLUSTER_STEP;
	CLUSTER_STEP_UNTIL_VOTED_FOR(k, i, 2000);


	//after server k grant server i‘s RV, then isolate server i.
	//server j still not receive RV, and it already election_timeout when
	//cluster time equal 500ms, and be the candidate and send RV out
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	ASSERT_TIME(600);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(i);

	/* Server 1 tick send RV */
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_REQUEST_VOTE), == ,0);
	munit_assert_int(CLUSTER_N_SEND(j, RAFT_IO_REQUEST_VOTE), == ,2);


	//wait for server j election_timeout and be another candidate

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	return MUNIT_OK;
}

//test candidate recv a AE which term bigger than itself,
//then it change state to follower and update it's term
TEST(paper_test, candidateFallBack, setUp, tearDown, 0, NULL) {
	struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_ELECT(k);
	ASSERT_LEADER(k);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);

	//let server k isolate from cluster
	CLUSTER_SATURATE_BOTHWAYS(k,j);
	CLUSTER_SATURATE_BOTHWAYS(k,i);
	CLUSTER_STEP_UNTIL_STATE_IS(k, RAFT_FOLLOWER, 2000);
	CLUSTER_STEP_UNTIL_STATE_IS(k, RAFT_CANDIDATE, 2000);
	raft_term t1 = CLUSTER_TERM(k);

	CLUSTER_STEP_UNTIL_STATE_IS(i, RAFT_LEADER, 2000);
	raft_term t2 = CLUSTER_TERM(i);
	munit_assert_llong(t1, <, t2);

	struct raft_entry entry1;
	entry1.type = RAFT_COMMAND;
	entry1.term = t2;
	FsmEncodeSetX(123, &entry1.buf);
	CLUSTER_ADD_ENTRY(i, &entry1);
	CLUSTER_DESATURATE_BOTHWAYS(k,i);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);
	ASSERT_TERM(k,t2);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}
