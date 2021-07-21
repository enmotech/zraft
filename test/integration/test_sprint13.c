#include "../../src/configuration.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../../src/election.h"
#include "../../src/log.h"
#include "../../src/progress.h"

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
SUITE(sprint13_tsl)

static void test_free_req(struct raft_apply *req, int status, void *result)
{
	(void)status;
	free(result);
	free(req);
}
#include <unistd.h>
//leader start replication, then check its progress: the State, Match Index,
//Next Index.
TEST(sprint13_tsl, progressLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;

	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	
	struct raft_progress *pr = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	munit_assert_not_null(pr);

	//the init mode is probe
	munit_assert_int(pr->state, ==, PROGRESS__PROBE);

	//first heartbeat response
	struct raft_append_entries_result res = {
		.term = 2,
		.rejected = 0,
		.last_log_index = 1 
	};
	CLUSTER_STEP_UNTIL_AE_RES(j, i, &res, 200);

	//leader recv first heartbeat response and start pipeline mode
	CLUSTER_STEP_UNTIL_ELAPSED(16);
	munit_assert_int(pr->state, ==, PROGRESS__PIPELINE);

	raft_index next_before = pr->next_index;

	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(i, req, 1, test_free_req);

	//leader start replication
	struct raft_append_entries ae = {
		.prev_log_term = 1,
		.prev_log_index = 1,
		.term = 2,
		.n_entries = 1
	};
	CLUSTER_STEP_UNTIL_AE(i, j, &ae, 200);

	//with pipeline mode, once push the AE msg into the queue,
	//the leader will add next_index for more efficiently
	munit_assert_llong(next_before+1, ==, pr->next_index);

	//before recv results
	raft_index match_before = pr->match_index;

	//follower send AE_RESULTS
	res.last_log_index = 2; 
	CLUSTER_STEP_UNTIL_AE_RES(j, i, &res, 26);

	//after recv results
	CLUSTER_STEP_UNTIL_ELAPSED(16);
	munit_assert_llong(match_before+1, ==, pr->match_index);
	munit_assert_llong(next_before+1, ==, pr->next_index);

	return MUNIT_OK;
}

//test that when part of RV will be dropped, what the candidate will do
TEST(sprint13_tsl, leaderElection_minority, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);

	CLUSTER_STEP_UNTIL_ELAPSED(1001);
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TERM(i,2);
	//after one round election, all these RV has been dropped
	CLUSTER_STEP_UNTIL_ELAPSED(1001);
	
	//I still be candidate, and add term to 3
	ASSERT_CANDIDATE(i);
	ASSERT_TERM(i,3);
	
	return MUNIT_OK;
}

TEST(sprint13_tsl, leaderElection_majority, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);

	CLUSTER_STEP_UNTIL_ELAPSED(1000);
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TERM(i,2);

	//restore network
	CLUSTER_DESATURATE_BOTHWAYS(i,j);

	//after one round election
	CLUSTER_STEP_UNTIL_ELAPSED(30);
	
	//I will be leader, cause reach a majority
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	
	return MUNIT_OK;
}

TEST(sprint13_tsl, leaderElection_minority_prevote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	ASSERT_TERM(i, 1);
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);
	raft_set_pre_vote(CLUSTER_RAFT(i), true);
	CLUSTER_STEP_UNTIL_ELAPSED(1001);
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TERM(i, 1);
	//after one round election, all these RV has been dropped
	CLUSTER_STEP_UNTIL_ELAPSED(1001);
	
	//I still be candidate, but term still is 2
	ASSERT_CANDIDATE(i);
	ASSERT_TERM(i, 1);
	
	return MUNIT_OK;
}

TEST(sprint13_tsl, leaderElection_majority_prevote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);
	raft_set_pre_vote(CLUSTER_RAFT(i), true);
	ASSERT_TERM(i,1);

	CLUSTER_STEP_UNTIL_ELAPSED(1000);
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	ASSERT_TERM(i,1);

	//restore network
	CLUSTER_DESATURATE_BOTHWAYS(i,j);

	//after pre-vote election
	CLUSTER_STEP_UNTIL_ELAPSED(30);

	//still be candidate
	ASSERT_CANDIDATE(i);
	ASSERT_TERM(i,2);
	
	//after the real one round election
	CLUSTER_STEP_UNTIL_ELAPSED(30);

	//I will be leader, cause reach a majority
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_TERM(i, 2);
	
	return MUNIT_OK;
}

static char *cluster_3[] = {"3", NULL};
static char *voting_2[] = {"2", NULL};
static MunitParameterEnum cluster_3_with_1_standby_params[] = {
        {CLUSTER_N_PARAM, cluster_3},
        {CLUSTER_N_VOTING_PARAM, voting_2},
        {NULL, NULL}
};
//test that even a leaner has been timeout, it still be a follower
TEST(sprint13_tsl,
	leanerElectionTimeout,
	setUp,
	tearDown,
	0,
	cluster_3_with_1_standby_params)
{
	
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_SATURATE_BOTHWAYS(i,j);
	CLUSTER_SATURATE_BOTHWAYS(i,k);
	CLUSTER_SATURATE_BOTHWAYS(j,k);

	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	//wait for a max election_timeout
	CLUSTER_STEP_UNTIL_ELAPSED(2000);

	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_FOLLOWER(k);
	return MUNIT_OK;
}

TEST(sprint13_tsl, leaderCycle, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_DEPOSE;	
	CLUSTER_ELECT(j);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(k);

	CLUSTER_DEPOSE;	
	CLUSTER_ELECT(k);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	return MUNIT_OK;
}

TEST(sprint13_tsl, leaderCycle_prevote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	raft_set_pre_vote(CLUSTER_RAFT(i), true);
	raft_set_pre_vote(CLUSTER_RAFT(j), true);
	raft_set_pre_vote(CLUSTER_RAFT(k), true);
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_DEPOSE;	
	CLUSTER_ELECT(j);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(k);

	CLUSTER_DEPOSE;	
	CLUSTER_ELECT(k);
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	return MUNIT_OK;
}

//test that when a leader come out, it maybe overwrites the log of follower
//which index and term is higher than leader but uncommited
static struct raft_entry g_et_0[3];
static struct raft_entry g_et_1[3];
TEST(sprint13_tsl, leaderElctionOverWriteNewerLogs, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;

	//prepare persist entries
	for (unsigned a = 0; a < 3; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		FsmEncodeSetX(123, &g_et_0[a].buf);

		g_et_1[a].type = RAFT_COMMAND;
		FsmEncodeSetX(123, &g_et_1[a].buf);
	}

	g_et_0[0].term = 2;
	g_et_0[1].term = 3;
	g_et_0[2].term = 6;
	for (uint8_t a = 0; a < 3; a++)
		CLUSTER_ADD_ENTRY(i, &g_et_0[a]);

	g_et_1[0].term = 3;
	g_et_1[1].term = 4;
	g_et_1[2].term = 5;
	for (uint8_t a = 0; a < 3; a++)
		CLUSTER_ADD_ENTRY(j, &g_et_1[a]);

	//start cluster and elect I as leader
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);
	CLUSTER_START;
	CLUSTER_ELECT(i);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_TERM(i, 2);
	ASSERT_TERM(j, 2);
	CLUSTER_RAFT(i)->current_term = 6;
	CLUSTER_RAFT(j)->current_term = 6;

	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 2));
	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(i)->log, 3));
	munit_assert_llong(6, ==, logTermOf(&CLUSTER_RAFT(i)->log, 4));

	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(j)->log, 2));
	munit_assert_llong(4, ==, logTermOf(&CLUSTER_RAFT(j)->log, 3));
	munit_assert_llong(5, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);	
	munit_assert_llong(1, ==, CLUSTER_RAFT(j)->commit_index);	
	
	//a few moments later
	CLUSTER_STEP_UNTIL_ELAPSED(1000);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_TERM(i, 6);
	ASSERT_TERM(j, 6);

	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 2));
	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(i)->log, 3));
	munit_assert_llong(6, ==, logTermOf(&CLUSTER_RAFT(i)->log, 4));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(j)->log, 2));
	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(j)->log, 3));
	munit_assert_llong(6, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));

	munit_assert_llong(4, ==, CLUSTER_RAFT(i)->commit_index);	
	munit_assert_llong(4, ==, CLUSTER_RAFT(j)->commit_index);	
	return MUNIT_OK;
}

TEST(sprint13_tsl, leaderElctionOverWriteNewerLogsi_prevote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;

	//prepare persist entries
	for (unsigned a = 0; a < 3; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		FsmEncodeSetX(123, &g_et_0[a].buf);

		g_et_1[a].type = RAFT_COMMAND;
		FsmEncodeSetX(123, &g_et_1[a].buf);
	}

	g_et_0[0].term = 2;
	g_et_0[1].term = 3;
	g_et_0[2].term = 6;
	for (uint8_t a = 0; a < 3; a++)
		CLUSTER_ADD_ENTRY(i, &g_et_0[a]);

	g_et_1[0].term = 3;
	g_et_1[1].term = 4;
	g_et_1[2].term = 5;
	for (uint8_t a = 0; a < 3; a++)
		CLUSTER_ADD_ENTRY(j, &g_et_1[a]);

	//start cluster and elect I as leader
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);
	CLUSTER_START;
	//start pre-vote mode
	raft_set_pre_vote(CLUSTER_RAFT(i), true);
	CLUSTER_ELECT(i);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_TERM(i, 2);
	ASSERT_TERM(j, 2);
	CLUSTER_RAFT(i)->current_term = 6;
	CLUSTER_RAFT(j)->current_term = 6;

	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 2));
	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(i)->log, 3));
	munit_assert_llong(6, ==, logTermOf(&CLUSTER_RAFT(i)->log, 4));

	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(j)->log, 2));
	munit_assert_llong(4, ==, logTermOf(&CLUSTER_RAFT(j)->log, 3));
	munit_assert_llong(5, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);	
	munit_assert_llong(1, ==, CLUSTER_RAFT(j)->commit_index);	
	
	//a few moments later
	CLUSTER_STEP_UNTIL_ELAPSED(1000);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_TERM(i, 6);
	ASSERT_TERM(j, 6);

	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 2));
	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(i)->log, 3));
	munit_assert_llong(6, ==, logTermOf(&CLUSTER_RAFT(i)->log, 4));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(j)->log, 2));
	munit_assert_llong(3, ==, logTermOf(&CLUSTER_RAFT(j)->log, 3));
	munit_assert_llong(6, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));

	munit_assert_llong(4, ==, CLUSTER_RAFT(i)->commit_index);	
	munit_assert_llong(4, ==, CLUSTER_RAFT(j)->commit_index);	
	return MUNIT_OK;
}

//test that follower and candidate can vote
TEST(sprint13_tsl, voteFromAnyState, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;
	CLUSTER_START;
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);
	
	CLUSTER_STEP_UNTIL_ELAPSED(1000);

	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_STEP_UNTIL_ELAPSED(30);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);
	
	CLUSTER_STEP_UNTIL_ELAPSED(2000);

	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	ASSERT_TERM(i, 3);
	ASSERT_TERM(j, 3);
	ASSERT_TERM(k, 3);

	CLUSTER_RAFT(i)->current_term = 4;

	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);

	CLUSTER_STEP_UNTIL_ELAPSED(30);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}

TEST(sprint13_tsl, voteFromAnyState_prevote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;


	return MUNIT_OK;
}
