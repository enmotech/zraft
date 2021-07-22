#include "../../src/configuration.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../../src/election.h"
#include "../../src/log.h"
#include "../../src/progress.h"
#include "../../src/byte.h"
#include "../../src/replication.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
	FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	SETUP_CLUSTER(3);
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
SUITE(etcd_migrate)

static void test_free_req(struct raft_apply *req, int status, void *result)
{
	(void)status;
	free(result);
	free(req);
}

//leader start replication, then check its progress: the State, Match Index,
//Next Index.
TEST(etcd_migrate, progressLeader, setUp, tearDown, 0, NULL)
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
TEST(etcd_migrate, leaderElectionMinority, setUp, tearDown, 0, NULL)
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

TEST(etcd_migrate, leaderElectionMajority, setUp, tearDown, 0, NULL)
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

TEST(etcd_migrate, leaderElectionMinorityPreVote, setUp, tearDown, 0, NULL)
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

TEST(etcd_migrate, leaderElectionMajorityPreVote, setUp, tearDown, 0, NULL)
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
TEST(etcd_migrate,
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

TEST(etcd_migrate, leaderCycle, setUp, tearDown, 0, NULL)
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

TEST(etcd_migrate, leaderCyclePreVote, setUp, tearDown, 0, NULL)
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
TEST(etcd_migrate, leaderElctionOverWriteNewerLogs, setUp, tearDown, 0, NULL)
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

TEST(etcd_migrate, leaderElctionOverWriteNewerLogsPreVote, setUp, tearDown, 0, NULL)
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
TEST(etcd_migrate, voteFromAnyState, setUp, tearDown, 0, NULL)
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

	//I be the leader, cause follower vote for it.
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);
	
	CLUSTER_STEP_UNTIL_ELAPSED(1300);

	ASSERT_FOLLOWER(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	ASSERT_TERM(i, 2);
	ASSERT_TERM(j, 3);
	ASSERT_TERM(k, 3);

	//mock a higher term.
	CLUSTER_RAFT(i)->current_term = 3;
	CLUSTER_STEP_UNTIL_ELAPSED(700);

	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	ASSERT_TERM(i, 4);
	ASSERT_TERM(j, 3);
	ASSERT_TERM(k, 3);

	CLUSTER_DESATURATE_BOTHWAYS(i, j);
	CLUSTER_DESATURATE_BOTHWAYS(i, k);
	CLUSTER_DESATURATE_BOTHWAYS(j, k);

	CLUSTER_STEP_UNTIL_ELAPSED(30);

	//I be the leader, cause candidate vote for it.
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	return MUNIT_OK;
}

TEST(etcd_migrate, voteFromAnyStatePreVote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;

	CLUSTER_START;
	ASSERT_FOLLOWER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	//set all server pre-vote true
	raft_set_pre_vote(CLUSTER_RAFT(i), true);	
	raft_set_pre_vote(CLUSTER_RAFT(j), true);	
	raft_set_pre_vote(CLUSTER_RAFT(k), true);	

	CLUSTER_STEP_UNTIL_ELAPSED(1000);

	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_STEP_UNTIL_ELAPSED(30);

	//after pre-vote
	ASSERT_CANDIDATE(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_STEP_UNTIL_ELAPSED(30);

	//after the real vote
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	CLUSTER_SATURATE_BOTHWAYS(i, j);
	CLUSTER_SATURATE_BOTHWAYS(i, k);
	CLUSTER_SATURATE_BOTHWAYS(j, k);
	
	CLUSTER_STEP_UNTIL_ELAPSED(1300);

	ASSERT_FOLLOWER(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	ASSERT_TERM(i, 2);
	ASSERT_TERM(j, 2);
	ASSERT_TERM(k, 2);

	//mock a higher term
	CLUSTER_RAFT(i)->current_term = 3;
	CLUSTER_STEP_UNTIL_ELAPSED(700);

	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	ASSERT_TERM(i, 3);
	ASSERT_TERM(j, 2);
	ASSERT_TERM(k, 2);

	CLUSTER_DESATURATE_BOTHWAYS(i, j);
	CLUSTER_DESATURATE_BOTHWAYS(i, k);
	CLUSTER_DESATURATE_BOTHWAYS(j, k);

	//after pre-vote
	CLUSTER_STEP_UNTIL_ELAPSED(30);

	ASSERT_CANDIDATE(i);
	ASSERT_CANDIDATE(j);
	ASSERT_CANDIDATE(k);
	ASSERT_TERM(i, 4);
	ASSERT_TERM(j, 2);
	ASSERT_TERM(k, 2);

	//after real vote
	CLUSTER_STEP_UNTIL_ELAPSED(30);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	ASSERT_TERM(i, 4);
	ASSERT_TERM(j, 4);
	ASSERT_TERM(k, 4);

	return MUNIT_OK;
}

TEST(etcd_migrate, logReplication, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
	unsigned i=0,j=1,k=2;

	//prepare persist entries
	for (unsigned a = 0; a < 3; a++) {
		g_et_0[a].term = 2;
		g_et_0[a].type = RAFT_COMMAND;
		FsmEncodeSetX((a+1)*111, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 3; a++)
		CLUSTER_ADD_ENTRY(i, &g_et_0[a]);
	

	CLUSTER_START;

	CLUSTER_STEP_UNTIL_ELAPSED(1030);
	
	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	ASSERT_FOLLOWER(k);

	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(1, ==, logLastIndex(&CLUSTER_RAFT(j)->log));
	munit_assert_llong(1, ==, logLastIndex(&CLUSTER_RAFT(k)->log));

	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 2));
	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 3));
	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 4));

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);	
	munit_assert_llong(1, ==, CLUSTER_RAFT(j)->commit_index);	
	munit_assert_llong(1, ==, CLUSTER_RAFT(k)->commit_index);	

	CLUSTER_STEP_UNTIL_ELAPSED(1000);

	//all those logs has been replicated to followers
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(k)->log));

	//check all the commit_index
	munit_assert_llong(4, ==, CLUSTER_RAFT(i)->commit_index);	
	munit_assert_llong(4, ==, CLUSTER_RAFT(j)->commit_index);	
	munit_assert_llong(4, ==, CLUSTER_RAFT(k)->commit_index);	

	//check the log data
	void *base = NULL;
	for (unsigned a = 1; a < 4; a++) {
		base = (char *)CLUSTER_RAFT(i)->log.entries[a].buf.base + 8;
		munit_assert_uint64(a*111, ==, byteGet64((const void **)&base));

		base = (char *)CLUSTER_RAFT(j)->log.entries[a].buf.base + 8;
		munit_assert_uint64(a*111, ==, byteGet64((const void **)&base));

		base = (char *)CLUSTER_RAFT(k)->log.entries[a].buf.base + 8;
		munit_assert_uint64(a*111, ==, byteGet64((const void **)&base));
	}
	
	return MUNIT_OK;
}

static char *cluster_1[] = {"1", NULL};
static MunitParameterEnum cluster_1_params[] = {
	{CLUSTER_N_PARAM, cluster_1},
	{NULL, NULL},
};
//test that single node cluster also can commit logs
TEST(etcd_migrate, singleNodeCommit, setUp, tearDown, 0, cluster_1_params)
{
	struct fixture *f = data;
	//add some entries
	for (unsigned a = 0; a < 3; a++) {
		g_et_0[a].term = 1;
		g_et_0[a].type = RAFT_COMMAND;
		FsmEncodeSetX((a+1)*111, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 3; a++)
		CLUSTER_ADD_ENTRY(0, &g_et_0[a]);

	CLUSTER_START;		
	ASSERT_LEADER(0);

	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(0)->log));
	munit_assert_llong(4, ==, CLUSTER_RAFT(0)->commit_index);	

	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(0, req, 1, test_free_req);

	CLUSTER_STEP_UNTIL_ELAPSED(10);

	munit_assert_llong(5, ==, logLastIndex(&CLUSTER_RAFT(0)->log));
	munit_assert_llong(5, ==, CLUSTER_RAFT(0)->commit_index);	

	return MUNIT_OK;
}

static char *cluster_2[] = {"2", NULL};
static MunitParameterEnum cluster_2_params[] = {
	{CLUSTER_N_PARAM, cluster_2},
	{NULL, NULL},
};

TEST(etcd_migrate, ignoreOldTermMessage, setUp, tearDown, 0, cluster_2_params)
{
	struct fixture *f = data;
	unsigned i = 0,j = 1;
	struct raft_append_entries *ae_ptr = NULL;

	CLUSTER_START;
	CLUSTER_STEP_UNTIL_ELAPSED(1030);

	ASSERT_LEADER(i);
	ASSERT_FOLLOWER(j);
	
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(i, req, 1, test_free_req);
	
	struct raft_append_entries ae = {
		.prev_log_term = 1,
		.prev_log_index = 1,
		.term = 2,
		.n_entries = 1
	};
	CLUSTER_STEP_UNTIL_AE(i, j, &ae, 200);
	ae_ptr = raft_fixture_get_ae_req(&f->cluster, i, j, &ae);	
	munit_assert_not_null(ae_ptr);

	//mock a lower term
	ae_ptr->term = 1;	

	CLUSTER_STEP_UNTIL_ELAPSED(15);

	//J ignore this AE, so that its last log index still be 1
	munit_assert_llong(1, ==, logLastIndex(&CLUSTER_RAFT(j)->log));
	return MUNIT_OK;
}

//test that single node leader commit
TEST(etcd_migrate, commitWithSingleNode, setUp, tearDown, 0, cluster_1_params)
{
	struct fixture *f = data;
	
	CLUSTER_START;
	ASSERT_LEADER(0);
	munit_assert_llong(1, ==, CLUSTER_RAFT(0)->commit_index);

	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(0, req, 1, test_free_req);

	//mock a higher term
	CLUSTER_RAFT(0)->current_term = 2;
	CLUSTER_STEP_UNTIL_ELAPSED(10);

	replicationQuorum(CLUSTER_RAFT(0), 2);
	munit_assert_llong(1, ==, CLUSTER_RAFT(0)->commit_index);

	struct raft_apply *req1 = munit_malloc(sizeof *req1);
	CLUSTER_APPLY_ADD_X(0, req1, 1, test_free_req);
	CLUSTER_STEP_UNTIL_ELAPSED(10);

	//leader commit all the log
	replicationQuorum(CLUSTER_RAFT(0), 3);
	munit_assert_llong(3, ==, CLUSTER_RAFT(0)->commit_index);

	return MUNIT_OK;
}


static char *cluster_4[] = {"4", NULL};
static MunitParameterEnum cluster_4_params[] = {
	{CLUSTER_N_PARAM, cluster_4},
	{NULL, NULL},
};
extern void replicationQuorum(struct raft *r, const raft_index index);

//test leader change commit index by check progress 
TEST(etcd_migrate, commitWithOddNodeMatchTerm, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i = 0,j = 1,k = 2;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	
	struct raft_progress *pr_i = &(CLUSTER_RAFT(i)->leader_state.progress[i]);
	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	struct raft_progress *pr_k = &(CLUSTER_RAFT(i)->leader_state.progress[k]);
	munit_assert_not_null(pr_i);
	munit_assert_not_null(pr_j);
	munit_assert_not_null(pr_k);
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(0, req, 1, test_free_req);

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->last_stored);
	CLUSTER_STEP_UNTIL_ELAPSED(10);
	munit_assert_llong(2, ==, CLUSTER_RAFT(i)->last_stored);

	pr_i->match_index = 2;
	pr_j->match_index = 1;
	pr_k->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	pr_i->match_index = 2;
	pr_j->match_index = 2;
	pr_k->match_index = 1;

	replicationQuorum(CLUSTER_RAFT(i), 2);

	munit_assert_llong(2, ==, CLUSTER_RAFT(i)->commit_index);
	return MUNIT_OK;
}

TEST(etcd_migrate, commitWithOddNodeHigherTerm, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned i = 0,j = 1,k = 2;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	
	struct raft_progress *pr_i = &(CLUSTER_RAFT(i)->leader_state.progress[i]);
	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	struct raft_progress *pr_k = &(CLUSTER_RAFT(i)->leader_state.progress[k]);
	munit_assert_not_null(pr_i);
	munit_assert_not_null(pr_j);
	munit_assert_not_null(pr_k);
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(0, req, 1, test_free_req);

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->last_stored);
	CLUSTER_STEP_UNTIL_ELAPSED(10);
	munit_assert_llong(2, ==, CLUSTER_RAFT(i)->last_stored);

	//mock a higher term
	CLUSTER_RAFT(i)->current_term = 3;

	pr_i->match_index = 2;
	pr_j->match_index = 1;
	pr_k->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since not reach majority
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	pr_i->match_index = 2;
	pr_j->match_index = 2;
	pr_k->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since leader won't commit preceding log 
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);
	return MUNIT_OK;
}

TEST(etcd_migrate, commitWithEvenNodeMatchTerm, setUp, tearDown, 0, cluster_4_params)
{
	struct fixture *f = data;
	unsigned i = 0,j = 1,k = 2, l = 3;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	
	struct raft_progress *pr_i = &(CLUSTER_RAFT(i)->leader_state.progress[i]);
	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	struct raft_progress *pr_k = &(CLUSTER_RAFT(i)->leader_state.progress[k]);
	struct raft_progress *pr_l = &(CLUSTER_RAFT(i)->leader_state.progress[l]);
	munit_assert_not_null(pr_i);
	munit_assert_not_null(pr_j);
	munit_assert_not_null(pr_k);
	munit_assert_not_null(pr_l);
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(0, req, 1, test_free_req);

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->last_stored);
	CLUSTER_STEP_UNTIL_ELAPSED(10);
	munit_assert_llong(2, ==, CLUSTER_RAFT(i)->last_stored);

	pr_i->match_index = 2;
	pr_j->match_index = 1;
	pr_k->match_index = 1;
	pr_l->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since not reach majority
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	pr_i->match_index = 2;
	pr_j->match_index = 2;
	pr_k->match_index = 1;
	pr_l->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since not reach majority
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	pr_i->match_index = 2;
	pr_j->match_index = 2;
	pr_k->match_index = 2;
	pr_l->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index will be 2 since reach a majority
	munit_assert_llong(2, ==, CLUSTER_RAFT(i)->commit_index);

	return MUNIT_OK;
}

TEST(etcd_migrate, commitWithEvenNodeHigherTerm, setUp, tearDown, 0, cluster_4_params)
{
	struct fixture *f = data;
	unsigned i = 0,j = 1,k = 2, l = 3;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	
	struct raft_progress *pr_i = &(CLUSTER_RAFT(i)->leader_state.progress[i]);
	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	struct raft_progress *pr_k = &(CLUSTER_RAFT(i)->leader_state.progress[k]);
	struct raft_progress *pr_l = &(CLUSTER_RAFT(i)->leader_state.progress[l]);
	munit_assert_not_null(pr_i);
	munit_assert_not_null(pr_j);
	munit_assert_not_null(pr_k);
	munit_assert_not_null(pr_l);
	struct raft_apply *req = munit_malloc(sizeof *req);
	CLUSTER_APPLY_ADD_X(0, req, 1, test_free_req);

	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->last_stored);
	CLUSTER_STEP_UNTIL_ELAPSED(10);
	munit_assert_llong(2, ==, CLUSTER_RAFT(i)->last_stored);

	//mock a higher term
	CLUSTER_RAFT(i)->current_term = 3;

	pr_i->match_index = 2;
	pr_j->match_index = 1;
	pr_k->match_index = 1;
	pr_l->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since not reach majority
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	pr_i->match_index = 2;
	pr_j->match_index = 2;
	pr_k->match_index = 1;
	pr_l->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since not reach majority
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	pr_i->match_index = 2;
	pr_j->match_index = 2;
	pr_k->match_index = 2;
	pr_l->match_index = 1;
	replicationQuorum(CLUSTER_RAFT(i), 2);

	//commit index still be 1 since leader won't commit preceding log 
	munit_assert_llong(1, ==, CLUSTER_RAFT(i)->commit_index);

	return MUNIT_OK;
}
