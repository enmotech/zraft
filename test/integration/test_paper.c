#include "../../src/configuration.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../../src/election.h"
#include "../../src/log.h"

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

	//define election_timeout
	raft_fixture_set_randomized_election_timeout(&f->cluster, i, 400);
	raft_fixture_set_randomized_election_timeout(&f->cluster, j, 500);
	raft_fixture_set_randomized_election_timeout(&f->cluster, k, 2000);
	raft_set_election_timeout(CLUSTER_RAFT(i), 400);
	raft_set_election_timeout(CLUSTER_RAFT(j), 500);
	raft_set_election_timeout(CLUSTER_RAFT(k), 2000);
	CLUSTER_SET_NETWORK_LATENCY(i,200);
	CLUSTER_SET_NETWORK_LATENCY(j,200);

	CLUSTER_START;

	CLUSTER_STEP_UNTIL_STATE_IS(i, RAFT_CANDIDATE,2000);
	//server 0 election timeout, then be the first candidate
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TIME(400);

	//when time is 500ms, the server j election_timeout and be another candidate,
	//and it still not receive the RV from i
	CLUSTER_STEP_UNTIL_STATE_IS(j, RAFT_CANDIDATE,2000);
	munit_assert_int(CLUSTER_N_SEND(i, RAFT_IO_REQUEST_VOTE), == ,2);
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_REQUEST_VOTE), == ,0);
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(k);
	ASSERT_TIME(500);

	/* Server k grant i's RV and server j's RV still in network */
	CLUSTER_STEP_UNTIL_VOTED_FOR(k, i, 2000);
	munit_assert_int(CLUSTER_N_SEND(j, RAFT_IO_REQUEST_VOTE), == ,2);
	ASSERT_TIME(600);

	//isolate server i
	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_STEP_UNTIL_ELAPSED(300);
	//server j already receive the RV_RESULT from k, but is not granted,
	//so server j still be the candidate
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_REQUEST_VOTE_RESULT), == ,1);
	ASSERT_CANDIDATE(j);

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

// leaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
//expect A. it wins the election
//expect B. it loses the election
//expect C. it is unclear about the result
TEST(paper_test, leaderElectionInOneRoundRPC, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;

	//define election_timeout for control the election
	raft_fixture_set_randomized_election_timeout(&f->cluster, i, 400);
	raft_fixture_set_randomized_election_timeout(&f->cluster, j, 500);
	raft_fixture_set_randomized_election_timeout(&f->cluster, k, 2000);
	raft_set_election_timeout(CLUSTER_RAFT(i), 400);
	raft_set_election_timeout(CLUSTER_RAFT(j), 500);
	raft_set_election_timeout(CLUSTER_RAFT(k), 2000);
	CLUSTER_SET_NETWORK_LATENCY(i,200);
	CLUSTER_SET_NETWORK_LATENCY(j,200);

	CLUSTER_START;

	CLUSTER_STEP_UNTIL_STATE_IS(i, RAFT_CANDIDATE,2000);
	//I election timeout, then be the first candidate
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TIME(400);

	//when time goes to 500ms, J election_timeout and be another candidate,
	//and it still not receive the RV from I cause I hold a 200ms network_latency
	CLUSTER_STEP_UNTIL_STATE_IS(j, RAFT_CANDIDATE,2000);
	munit_assert_int(CLUSTER_N_SEND(i, RAFT_IO_REQUEST_VOTE), == ,2);
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_REQUEST_VOTE), == ,0);
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(k);
	ASSERT_TIME(500);

	/* K granted I's RV */
	CLUSTER_STEP_UNTIL_VOTED_FOR(k, i, 2000);

	//make sure K only receive RV from I, cause the J's RV still
	//propagating through the network
	munit_assert_int(CLUSTER_N_RECV(k, RAFT_IO_REQUEST_VOTE), == ,1);

	//make sure J already send RV out
	munit_assert_int(CLUSTER_N_SEND(j, RAFT_IO_REQUEST_VOTE), == ,2);
	ASSERT_TIME(600);

	//isolate I from the network for avoid win the election
	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_STEP_UNTIL_ELAPSED(300);
	//J already receive the RV_RESULT from K, but is not granted, cause K already grant for I just now.
	//so J remains candidate state
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_REQUEST_VOTE_RESULT), == ,1);
	//after One Round RV, neither candidate I nor candidate J achieve a majority,
	// so both of them remain candidate state (expect C)
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(i);
	ASSERT_TIME(900);

	raft_term t1 = CLUSTER_TERM(i);
	raft_term t2 = CLUSTER_TERM(j);
	//first one round RV of candidate I already timeout,
	// it must add term and start a new term election
	munit_assert_llong(t1, ==, t2+1);
	CLUSTER_STEP_UNTIL_ELAPSED(300);

	//recover the msg communication of I and set a lower network latency
	//for guarantee I's RV will be received firstly
	CLUSTER_DESATURATE_BOTHWAYS(i, j);
	CLUSTER_DESATURATE_BOTHWAYS(i, k);
	CLUSTER_SET_NETWORK_LATENCY(i,15);

	CLUSTER_STEP_UNTIL_ELAPSED(100);
	ASSERT_LEADER(i);	//expect A
	ASSERT_FOLLOWER(j); //expect B
	ASSERT_TERM(i, 4);
	return MUNIT_OK;
}

/* Implementation of raft_io->random. */
static int test_random(struct raft_io *io, int min, int max)
{
	(void)io;
	return min + (abs(rand()) % (max - min));
}

//test when state change to follower,
//different server's election_timeout will be randomized to a different number
TEST(paper_test, followerElectionTimeoutRandomized, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_RAFT(i)->io->random = test_random;
	CLUSTER_RAFT(j)->io->random = test_random;
	CLUSTER_RAFT(k)->io->random = test_random;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);
	CLUSTER_START;
	CLUSTER_STEP_UNTIL_ELAPSED(2000);
	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	CLUSTER_DESATURATE_BOTHWAYS(j,k);
	CLUSTER_DESATURATE_BOTHWAYS(i,j);
	CLUSTER_DESATURATE_BOTHWAYS(i,k);

	CLUSTER_STEP_UNTIL_HAS_LEADER(3000);
	unsigned  l = CLUSTER_LEADER;
	unsigned  m, n;
	switch(l) {
		case 0:
			m = 1;
			n = 2;
			break;
		case 1:
			m = 0;
			n = 2;
			break;
		case 2:
			m = 0;
			n = 1;
			break;
	}
	ASSERT_FOLLOWER(m);
	ASSERT_FOLLOWER(n);
	int t1 = CLUSTER_RAFT(m)->follower_state.randomized_election_timeout;
	int t2 = CLUSTER_RAFT(n)->follower_state.randomized_election_timeout;
	munit_assert_int(t1, != ,t2);
	return MUNIT_OK;
}

//test when state change to candidate,
//different server's election_timeout will be randomized to a different number
TEST(paper_test, candidateElectionTimeoutRandomized, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_RAFT(i)->io->random = test_random;
	CLUSTER_RAFT(j)->io->random = test_random;
	CLUSTER_RAFT(k)->io->random = test_random;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);
	CLUSTER_START;
	CLUSTER_STEP_UNTIL_ELAPSED(2000);
	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);

	int t1 = CLUSTER_RAFT(i)->candidate_state.randomized_election_timeout;
	int t2 = CLUSTER_RAFT(j)->candidate_state.randomized_election_timeout;
	int t3 = CLUSTER_RAFT(k)->candidate_state.randomized_election_timeout;

	munit_assert_int(t1, !=, t2);
	munit_assert_int(t1, !=, t3);
	munit_assert_int(t2, !=, t3);
	return MUNIT_OK;
}

TEST(paper_test, followerElectionTimeoutNonconflict, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_RAFT(i)->io->random = test_random;
	CLUSTER_RAFT(j)->io->random = test_random;
	CLUSTER_RAFT(k)->io->random = test_random;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);
	CLUSTER_START;
	CLUSTER_STEP_UNTIL_ELAPSED(2000);
	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);

	//recover network for produce a leader
	CLUSTER_DESATURATE_BOTHWAYS(i,j);
	CLUSTER_DESATURATE_BOTHWAYS(i,k);
	CLUSTER_DESATURATE_BOTHWAYS(j,k);
	CLUSTER_STEP_UNTIL_HAS_LEADER(3000);

	//find out the leader and followers
	unsigned  l = CLUSTER_LEADER;
	unsigned  m, n;
	switch(l) {
		case 0:
			m = 1;
			n = 2;
			break;
		case 1:
			m = 0;
			n = 2;
			break;
		case 2:
			m = 0;
			n = 1;
			break;
	}
	ASSERT_FOLLOWER(m);
	ASSERT_FOLLOWER(n);

	//saturate again the leader
	CLUSTER_SATURATE_BOTHWAYS(l, m);
	CLUSTER_SATURATE_BOTHWAYS(l, n);

	//find out the server with the minimal election_timeout
	int min_idx = m;
	int another = n;
	int min_et = CLUSTER_RAFT(m)->follower_state.randomized_election_timeout;
	if (min_et > (int)CLUSTER_RAFT(n)->candidate_state.randomized_election_timeout) {
		min_idx = n;
		another = m;
	}

	//when the minimal election_timeout happen, another server most possibly
	//still be follower cause the randomized election_timeout which avoid split votes
	CLUSTER_STEP_UNTIL_STATE_IS(min_idx, RAFT_CANDIDATE, 2000);
	ASSERT_FOLLOWER(another);
	return MUNIT_OK;
}

TEST(paper_test, candidateElectionTimeoutNonconflict, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_RAFT(i)->io->random = test_random;
	CLUSTER_RAFT(j)->io->random = test_random;
	CLUSTER_RAFT(k)->io->random = test_random;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);
	CLUSTER_START;
	CLUSTER_STEP_UNTIL_ELAPSED(2000);
	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	raft_term t1 = CLUSTER_TERM(i);
	raft_term t2 = CLUSTER_TERM(i);
	raft_term t3 = CLUSTER_TERM(i);
	munit_assert_llong(t1, ==, t2);
	munit_assert_llong(t1, ==, t3);
	int et[3] = {0};
	et[i] = CLUSTER_RAFT(i)->candidate_state.randomized_election_timeout;
	et[j] = CLUSTER_RAFT(j)->candidate_state.randomized_election_timeout;
	et[k] = CLUSTER_RAFT(k)->candidate_state.randomized_election_timeout;

	//select the minimal election_timeout server
	int min_et = et[i],
		min_idx = i;
	for (int idx = 1; idx < 3; idx++) {
		if (et[idx] < min_et) {
			min_et = et[idx];
			min_idx = idx;
		}
	}

	//wait for it's new term, indicate it start a new round request vote
	CLUSTER_STEP_UNTIL_TERM_IS(min_idx, t1+1, 2000);

	//randomized election_timeout to ensure that,
	//at the same time, only one server will election_timeout and
	//start request_vote_rpc for avoid split votes
	switch (min_idx) {
		case 0:
			ASSERT_TERM(1,t1);
			ASSERT_TERM(2,t1);
			break;
		case 1:
			ASSERT_TERM(0,t1);
			ASSERT_TERM(2,t1);
			break;
		case 2:
			ASSERT_TERM(0,t1);
			ASSERT_TERM(1,t1);
			break;
	}

	return MUNIT_OK;
}

// tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
static void test_free_req(struct raft_apply *req, int status, void *result)
{
	(void)status;
	free(result);
	free(req);
}

TEST(paper_test, leaderStartReplication, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//the leader append an entry, and replicate to all the followers
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(i, req, 1, test_free_req);
	CLUSTER_STEP_UNTIL_DELIVERED(i, j, 100);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);

	//make sure the follower recv the append entry
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_APPEND_ENTRIES), == ,1);
	munit_assert_int(CLUSTER_N_RECV(k, RAFT_IO_APPEND_ENTRIES), == ,1);

	return MUNIT_OK;
}


//when leader recv enough AE_RESULTS, then it will apply the entry,
//then test leader return a commit to the client
TEST(paper_test, leaderCommitEntry, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//the leader append an entry, and replicate to all the followers
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(i, req, 1, test_free_req);
	CLUSTER_STEP_UNTIL_DELIVERED(i, j, 100);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);

	//make sure the follower recv the append entry
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_APPEND_ENTRIES), == ,1);
	munit_assert_int(CLUSTER_N_RECV(k, RAFT_IO_APPEND_ENTRIES), == ,1);

	CLUSTER_STEP_UNTIL_DELIVERED(j, i, 100);
	CLUSTER_STEP_UNTIL_DELIVERED(k, i, 100);

	//make sure the leader recv two AR_RESULT
	munit_assert_int(CLUSTER_N_RECV(i, RAFT_IO_APPEND_ENTRIES_RESULT), == ,2);

	//step leader apply the entry and update the commit index
	CLUSTER_STEP_UNTIL_APPLIED(i, 2, 2000);

	//make sure the entry set a commit state
	munit_assert_int(f->cluster.commit_index, ==, 2);

	return MUNIT_OK;
}

struct ae_cnt {
	unsigned i;
	unsigned n;
};

static bool server_send_n_append_entry(
	struct raft_fixture *f,
	void *arg)
{
	struct ae_cnt *a = arg;
	unsigned n = raft_fixture_n_send(f, a->i, RAFT_IO_APPEND_ENTRIES);
	return a->n == n;
}

static bool server_recv_n_append_entry(
	struct raft_fixture *f,
	void *arg)
{
	struct ae_cnt *a = arg;
	unsigned n = raft_fixture_n_recv(f, a->i, RAFT_IO_APPEND_ENTRIES);
	return a->n == n;
}

//after leader committed, the next heartbeat will notify follower to commit
TEST(paper_test, followerCommitEntry, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//the leader append an entry, and replicate to all the followers
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(i, req, 1, test_free_req);
	CLUSTER_STEP_UNTIL_DELIVERED(i, j, 100);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);

	//make sure the follower recv the append entry
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_APPEND_ENTRIES), == ,1);
	munit_assert_int(CLUSTER_N_RECV(k, RAFT_IO_APPEND_ENTRIES), == ,1);

	CLUSTER_STEP_UNTIL_DELIVERED(j, i, 100);
	CLUSTER_STEP_UNTIL_DELIVERED(k, i, 100);

	//make sure the leader recv two AR_RESULT
	munit_assert_int(CLUSTER_N_RECV(i, RAFT_IO_APPEND_ENTRIES_RESULT), == ,2);

	//step leader apply the entry and update the commit index
	CLUSTER_STEP_UNTIL_APPLIED(i, 2, 2000);

	//make sure the entry set a commit state
	munit_assert_int(f->cluster.servers[i].raft.commit_index, ==, 2);
	munit_assert_int(f->cluster.servers[j].raft.commit_index, ==, 1);

	//step until I send a heartbeat
	unsigned send_cnt = CLUSTER_N_SEND(i, RAFT_IO_APPEND_ENTRIES);
	unsigned recv_cnt = CLUSTER_N_RECV(j, RAFT_IO_APPEND_ENTRIES);
	struct ae_cnt arg = {i, send_cnt+1};
	CLUSTER_STEP_UNTIL(server_send_n_append_entry, &arg,200);
	arg.i = j;
	arg.n = recv_cnt+1;
	CLUSTER_STEP_UNTIL(server_recv_n_append_entry, &arg,200);

	//once the follower recv the heartbeat with new commit index,
	//then it immediately commit the same commit_index log
	munit_assert_int(f->cluster.servers[j].raft.commit_index, ==, 2);
	return MUNIT_OK;
}

//test once leader recv a majority, then it will start apply and set commit
TEST(paper_test, leaderAcknownledgeCommit, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//the leader append an entry, and replicate to all the followers
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(i, req, 1, test_free_req);
	CLUSTER_STEP_UNTIL_DELIVERED(i, j, 100);
	CLUSTER_STEP_UNTIL_DELIVERED(i, k, 100);

	//make sure the follower recv the append entry
	munit_assert_int(CLUSTER_N_RECV(j, RAFT_IO_APPEND_ENTRIES), == ,1);
	munit_assert_int(CLUSTER_N_RECV(k, RAFT_IO_APPEND_ENTRIES), == ,1);

	//ONLY recv one result, but still be a majority
	CLUSTER_STEP_UNTIL_DELIVERED(j, i, 100);

	//make sure the leader recv two AR_RESULT
	munit_assert_int(CLUSTER_N_RECV(i, RAFT_IO_APPEND_ENTRIES_RESULT), == ,1);

	//step leader apply the entry and update the commit index
	CLUSTER_STEP_UNTIL_APPLIED(i, 2, 2000);

	//make sure the entry set a commit state
	munit_assert_int(f->cluster.commit_index, ==, 2);

	return MUNIT_OK;
}


TEST(paper_test, leaderCommitPrecedingEntry, setUp, tearDown, 0, NULL)
{
	return MUNIT_OK;
}

TEST(paper_test, followerCheckMsgAPP, setUp, tearDown, 0, NULL)
{
	return MUNIT_OK;
}

TEST(paper_test, followerAppendEntry, setUp, tearDown, 0, NULL)
{
	return MUNIT_OK;
}

TEST(paper_test, leaderSyncFollowerLog, setUp, tearDown, 0, NULL)
{
	return MUNIT_OK;
}

struct ae_result_cnt {
	unsigned i;
	unsigned n;
};

static bool server_recv_n_append_entry_result(
	struct raft_fixture *f,
	void *arg)
{
	struct ae_result_cnt *a = arg;
	unsigned n = raft_fixture_n_recv(f, a->i, RAFT_IO_APPEND_ENTRIES_RESULT);
	return a->n == n;
}

//test the vote_request include the candidate's log and are sent to all of the other nodes
TEST(paper_test, requestVote, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	struct raft_apply *req1;
	struct raft_apply *req2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//the leader append two entries, and replicate to all the followers
	req1 = munit_malloc(sizeof(struct raft_apply));
	req2 = munit_malloc(sizeof(struct raft_apply));
	CLUSTER_APPLY_ADD_X(i, req1, 1, test_free_req);
	CLUSTER_APPLY_ADD_X(i, req2, 2, test_free_req);

	struct ae_result_cnt arg = {i, 4};
	CLUSTER_STEP_UNTIL(server_recv_n_append_entry_result, &arg,400);
	raft_term t1 = CLUSTER_TERM(i);

	//saturate for let I start a new election
	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);

	CLUSTER_STEP_UNTIL_STATE_IS(i, RAFT_CANDIDATE, 4000);
	CLUSTER_DESATURATE_BOTHWAYS(i, j);
	CLUSTER_DESATURATE_BOTHWAYS(i, k);

	//check candidate's RV detail
	raft_fixture_step_until_rv_for_send(
		&f->cluster, i, j, CLUSTER_TERM(i), t1, 3, 200);

	// all of the other nodes recv RV
	CLUSTER_N_RECV(j,RAFT_IO_REQUEST_VOTE);
	CLUSTER_N_RECV(k,RAFT_IO_REQUEST_VOTE);

	return MUNIT_OK;
}

//test voter response the request_vote and check the expect granted
TEST(paper_test, voter, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i=0, j=1, k=2;
	struct raft_apply *req1;
	struct raft_apply *req2;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//the leader append two entries, and replicate to all the followers
	req1 = munit_malloc(sizeof(struct raft_apply));
	req2 = munit_malloc(sizeof(struct raft_apply));
	CLUSTER_APPLY_ADD_X(i, req1, 1, test_free_req);
	CLUSTER_APPLY_ADD_X(i, req2, 2, test_free_req);

	struct ae_result_cnt arg = {i, 4};
	CLUSTER_STEP_UNTIL(server_recv_n_append_entry_result, &arg,400);
	raft_term t1 = logLastTerm(&f->cluster.servers[i].raft.log);
	raft_term t2 = logLastTerm(&f->cluster.servers[j].raft.log);
	raft_term t3 = logLastTerm(&f->cluster.servers[k].raft.log);
	raft_index i1 = logLastIndex(&f->cluster.servers[i].raft.log);
	raft_index i2 = logLastIndex(&f->cluster.servers[j].raft.log);
	raft_index i3 = logLastIndex(&f->cluster.servers[k].raft.log);
	munit_assert_llong(t1, ==, t2);
	munit_assert_llong(t1, ==, t3);
	munit_assert_llong(i1, ==, i2);
	munit_assert_llong(i1, ==, i3);

	//saturate for let J become candidate firstly
	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);
	raft_fixture_set_election_timeout_min(&f->cluster, j);
	CLUSTER_STEP_UNTIL_STATE_IS(j, RAFT_CANDIDATE, 1001);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(k);
	CLUSTER_DESATURATE_BOTHWAYS(i, j);
	CLUSTER_DESATURATE_BOTHWAYS(i, k);
	CLUSTER_DESATURATE_BOTHWAYS(j, k);

	//check candidate's RV detail
	raft_fixture_step_until_rv_for_send(
		&f->cluster, j, i, CLUSTER_TERM(j), t2, i2, 200);

	//set rv last_log_term - 1, so the voter will reject
	raft_fixture_step_rv_mock(&f->cluster, j, i, CLUSTER_TERM(j), t1-1, i2);
	ASSERT_FOLLOWER(i);
	raft_fixture_step_until_rv_response(&f->cluster, i, j, t1, false, 200);
	ASSERT_FOLLOWER(k);
	raft_fixture_step_until_rv_response(&f->cluster, k, j, t3, true, 200);
	return MUNIT_OK;
}

TEST(paper_test, leaderOnlyCommitLogFromCurrentTerm, setUp, tearDown, 0, NULL)
{
	return MUNIT_OK;
}