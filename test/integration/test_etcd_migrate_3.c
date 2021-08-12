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

static char *cluster_2[] = {"2", NULL};
static MunitParameterEnum cluster_2_params[] = {
	{CLUSTER_N_PARAM, cluster_2},
	{NULL, NULL},
};

static void test_free_req(struct raft_apply *req, int status, void *result)
{
	(void)status;
	free(result);
	free(req);
}


static struct raft_entry g_et_0[2];
//test leader recv ae-res and do appropriate reaction
TEST(etcd_migrate, leaderAppResp, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0,j=1;

	//prepare persist entries
	for (unsigned a = 0; a < 2; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		g_et_0[a].term = 2;
		FsmEncodeSetX(123, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 2; a++)
		CLUSTER_ADD_ENTRY(i, &g_et_0[a]);

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);

	struct raft_append_entries_result ae_res = {
		.term = 2,
		.last_log_index = 3,
		.rejected = 0
	};

	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	munit_assert_not_null(pr_j);
	
	CLUSTER_STEP_UNTIL_AE_RES(j, i, &ae_res, 200);

	return MUNIT_OK;
}

//test  member variable of heartbeat msg
TEST(etcd_migrate, bcastBeat, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0,j=1;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);

	struct raft_append_entries ae = {
		.prev_log_index = 1,
		.prev_log_term = 1,
		.leader_commit = 1,
		.term = 2,
		.n_entries = 0
	};
	
	CLUSTER_STEP_UNTIL_AE(i, j, &ae, 200);

	return MUNIT_OK;
}

//test that when progress state is pipeline,the next index is optimistically
TEST(etcd_migrate, increaseNext, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0,j=1;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);

	//wait for leader recv heartbeat response
	CLUSTER_STEP_UNTIL_ELAPSED(15);

	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	munit_assert_not_null(pr_j);
	munit_assert_llong(pr_j->next_index, ==, 2);
	munit_assert_llong(pr_j->match_index, ==, 1);
	munit_assert_llong(pr_j->state, ==, PROGRESS__PIPELINE);
	

	struct raft_apply *req = munit_malloc(sizeof *req);	
	CLUSTER_APPLY_ADD_N(i, req, 123, 10, test_free_req);

	munit_assert_llong(pr_j->next_index, ==, 12);
	munit_assert_llong(pr_j->match_index, ==, 1);
	return MUNIT_OK;
}

//test that before leader recv the first heartbeat response,
//the progress state will be probe and none entry will be sent.
//After receive first heartbeat response, 
//state will change to pipeline and send entries out
TEST(etcd_migrate, sendAppendForProgressProbe, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0,j=1;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);

	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	munit_assert_not_null(pr_j);
	munit_assert_llong(pr_j->next_index, ==, 2);
	munit_assert_llong(pr_j->match_index, ==, 0);

	//saturate from J to I for drop the heartbeat response
	CLUSTER_SATURATE(j, i);

	//wait for response drop and check progress state still be probe
	CLUSTER_STEP_UNTIL_ELAPSED(15);
	munit_assert_llong(pr_j->state, ==, PROGRESS__PROBE);
	munit_assert_llong(pr_j->next_index, ==, 2);
	munit_assert_llong(pr_j->match_index, ==, 0);

	//add ten entries, and none of them will be sent cause still be probe
	struct raft_apply *req = munit_malloc(sizeof *req);	
	CLUSTER_APPLY_ADD_N(i, req, 123, 10, test_free_req);

	CLUSTER_STEP_UNTIL_ELAPSED(30);
		
	munit_assert_llong(pr_j->state, ==, PROGRESS__PROBE);
	munit_assert_llong(pr_j->next_index, ==, 2);
	munit_assert_llong(pr_j->match_index, ==, 0);

	//restore network and step until next ae sent
	CLUSTER_DESATURATE(j, i);
	struct raft_append_entries ae = {
		.term = 2,
		.prev_log_index = 1,
		.prev_log_term = 1,
		.n_entries = 10
	};
	CLUSTER_STEP_UNTIL_AE(i, j, &ae, 200);
	munit_assert_llong(pr_j->state, ==, PROGRESS__PROBE);
	munit_assert_llong(pr_j->next_index, ==, 11);
	munit_assert_llong(pr_j->match_index, ==, 0);

	//wait for ae response
	CLUSTER_STEP_UNTIL_ELAPSED(30);

	//change to pipeline
	munit_assert_llong(pr_j->state, ==, PROGRESS__PIPELINE);
	munit_assert_llong(pr_j->next_index, ==, 12);
	munit_assert_llong(pr_j->match_index, ==, 11);

	return MUNIT_OK;
}

//test when state is pipeline, the ae entries could be more than one
TEST(etcd_migrate, sendAppendForProgressPipeline, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0,j=1;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);

	//wait for leader recv heartbeat response
	CLUSTER_STEP_UNTIL_ELAPSED(15);

	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	munit_assert_not_null(pr_j);
	munit_assert_llong(pr_j->state, ==, PROGRESS__PIPELINE);
	
	struct raft_apply *req = munit_malloc(sizeof *req);	
	CLUSTER_APPLY_ADD_N(i, req, 123, 10, test_free_req);
	
	struct raft_append_entries ae = {
		.term = 2,
		.prev_log_index = 1,
		.prev_log_term = 1,
		.n_entries = 10
	};
	
	//check the ae contain 10 entries
	CLUSTER_STEP_UNTIL_AE(i, j, &ae, 1);

	return MUNIT_OK;
}

//test pipeline state will change to probe state when msg can't reach to follower
//and check the index 
TEST(etcd_migrate, msgUnreachable, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0,j=1;

	CLUSTER_START;
	CLUSTER_ELECT(i);
	ASSERT_FOLLOWER(j);

	//wait for leader recv heartbeat response
	CLUSTER_STEP_UNTIL_ELAPSED(15);

	struct raft_progress *pr_j = &(CLUSTER_RAFT(i)->leader_state.progress[j]);
	munit_assert_not_null(pr_j);
	munit_assert_llong(pr_j->next_index, ==, 2);
	munit_assert_llong(pr_j->match_index, ==, 1);
	munit_assert_llong(pr_j->state, ==, PROGRESS__PIPELINE);
	
	//append 10 entries and replicate
	struct raft_apply *req = munit_malloc(sizeof *req);	
	CLUSTER_APPLY_ADD_N(i, req, 123, 10, test_free_req);

	munit_assert_llong(pr_j->next_index, ==, 12);
	munit_assert_llong(pr_j->match_index, ==, 1);

	//mock a errno so send operation will be treat as fail
	raft_fixture_mock_errno(-1);

	//wait for entries sent
	CLUSTER_STEP_UNTIL_ELAPSED(1);

	//check the probe state when msg unreachable
	munit_assert_llong(pr_j->state, ==, PROGRESS__PROBE);

	//and the next index will be match index plus 1
	munit_assert_llong(pr_j->next_index, ==, 2);
	munit_assert_llong(pr_j->match_index, ==, 1);
	return MUNIT_OK;
}

static struct raft_entry g_conf_et;
//restore from persist, check index and term and configuration
TEST(etcd_migrate, restore, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned i=0;

	munit_assert_uint(CLUSTER_RAFT(i)->configuration.n, ==, 0);

	//construct an entry for configuration
	g_conf_et.term = 1;
	g_conf_et.type = RAFT_CHANGE;
	raft_fixture_construct_configuration_log_buf(3, 3, &g_conf_et);

	//add this entry to io_impl persist
	CLUSTER_ADD_ENTRY(i, &g_conf_et);

	//add other normal entry
	for (unsigned a = 0; a < 2; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		g_et_0[a].term = 2;
		FsmEncodeSetX(123, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 2; a++)
		CLUSTER_ADD_ENTRY(i, &g_et_0[a]);

	//raft_start
	raft_start(CLUSTER_RAFT(i));

	//check the configuration restore to raft instance
	munit_assert_uint(CLUSTER_RAFT(i)->configuration.n, ==, 3);

	//check index and term
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(i)->log));
	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(i)->log, 4));
		
	return MUNIT_OK;
}

//restore from persist, check index and term and configuration
//besides, there exist a stand_by server
TEST(etcd_migrate, restoreWithStandby, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned j = 1;

	munit_assert_uint(CLUSTER_RAFT(j)->configuration.n, ==, 0);

	//construct an entry for configuration
	//and only the first server be voter
	g_conf_et.term = 1;
	g_conf_et.type = RAFT_CHANGE;
	raft_fixture_construct_configuration_log_buf(3, 1, &g_conf_et);

	//add this entry to io_impl persist
	CLUSTER_ADD_ENTRY(j, &g_conf_et);

	//add other normal entry
	for (unsigned a = 0; a < 2; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		g_et_0[a].term = 2;
		FsmEncodeSetX(123, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 2; a++)
		CLUSTER_ADD_ENTRY(j, &g_et_0[a]);

	//raft_start
	raft_start(CLUSTER_RAFT(j));

	//check the configuration restore to raft instance
	munit_assert_uint(CLUSTER_RAFT(j)->configuration.n, ==, 3);

	//check role is stand_by
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j].role, ==, RAFT_STANDBY);
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j+1].role, ==, RAFT_STANDBY);

	//check index and term
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));
	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));
		
	return MUNIT_OK;
}

//restore from persist, check index and term and configuration
//and the voter change to standby
TEST(etcd_migrate, restoreVoterToStandby, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned j = 1;

	//original contain contain 2 servers and 2 voters
	raft_fixture_construct_configuration(2, 2, &CLUSTER_RAFT(j)->configuration);
	munit_assert_uint(CLUSTER_RAFT(j)->configuration.n, ==, 2);
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j].role, ==, RAFT_VOTER);

	//construct an entry for configuration
	//and only the first server be voter
	g_conf_et.term = 1;
	g_conf_et.type = RAFT_CHANGE;
	raft_fixture_construct_configuration_log_buf(3, 1, &g_conf_et);

	//add this entry to io_impl persist
	CLUSTER_ADD_ENTRY(j, &g_conf_et);

	//add other normal entry
	for (unsigned a = 0; a < 2; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		g_et_0[a].term = 2;
		FsmEncodeSetX(123, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 2; a++)
		CLUSTER_ADD_ENTRY(j, &g_et_0[a]);

	//raft_start
	raft_start(CLUSTER_RAFT(j));

	//check the configuration restore to raft instance
	munit_assert_uint(CLUSTER_RAFT(j)->configuration.n, ==, 3);

	//check role is stand_by
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j].role, ==, RAFT_STANDBY);
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j+1].role, ==, RAFT_STANDBY);

	//check index and term
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));
	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));
		
	return MUNIT_OK;
}

//standby change to voter
TEST(etcd_migrate, restoreStandbyToVoter, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	unsigned j = 1;

	//original contain contain 2 servers and 2 voters
	raft_fixture_construct_configuration(2, 1, &CLUSTER_RAFT(j)->configuration);
	munit_assert_uint(CLUSTER_RAFT(j)->configuration.n, ==, 2);
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j].role, ==, RAFT_STANDBY);

	//construct an entry for configuration
	//and all servers be voter
	g_conf_et.term = 1;
	g_conf_et.type = RAFT_CHANGE;
	raft_fixture_construct_configuration_log_buf(3, 3, &g_conf_et);

	//add this entry to io_impl persist
	CLUSTER_ADD_ENTRY(j, &g_conf_et);

	//add other normal entry
	for (unsigned a = 0; a < 2; a++) {
		g_et_0[a].type = RAFT_COMMAND;
		g_et_0[a].term = 2;
		FsmEncodeSetX(123, &g_et_0[a].buf);
	}

	for (uint8_t a = 0; a < 2; a++)
		CLUSTER_ADD_ENTRY(j, &g_et_0[a]);

	//raft_start
	raft_start(CLUSTER_RAFT(j));

	//check the configuration restore to raft instance
	munit_assert_uint(CLUSTER_RAFT(j)->configuration.n, ==, 3);

	//check role is stand_by
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j].role, ==, RAFT_VOTER);
	munit_assert_int(CLUSTER_RAFT(j)->configuration.servers[j+1].role, ==, RAFT_VOTER);

	//check index and term
	munit_assert_llong(4, ==, logLastIndex(&CLUSTER_RAFT(j)->log));
	munit_assert_llong(2, ==, logTermOf(&CLUSTER_RAFT(j)->log, 4));
		
	return MUNIT_OK;
}

static void raft_change_cb_mock(struct raft_change *req, int status)
{
	(void)(req);
	(void)(status);
}


//test there will be a new log when config-change
//and check the umcommitted_index
struct raft_change g_raft_change_req[2];
TEST(etcd_migrate, stepConfig, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	struct raft *r = CLUSTER_RAFT(0);

	CLUSTER_START;
	CLUSTER_ELECT(0);

	//before config change
	munit_assert_llong(1, ==, logLastIndex(&r->log));
	munit_assert_llong(0, ==, r->configuration_uncommitted_index);

	//add one server
	int ret = raft_add(r, &g_raft_change_req[0], 4, "test_server", raft_change_cb_mock);
	munit_assert_int(ret, ==, 0);

	//check
	munit_assert_llong(2, ==, logLastIndex(&r->log));
	munit_assert_llong(2, ==, r->configuration_uncommitted_index);

	return MUNIT_OK;
}

//test a new conf-change will be ignore since the last conf-change log still be uncommitted
TEST(etcd_migrate, stepIgnoreConfig, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
	struct raft *r = CLUSTER_RAFT(0);

	CLUSTER_START;
	CLUSTER_ELECT(0);

	//before config change
	munit_assert_llong(1, ==, logLastIndex(&r->log));
	munit_assert_llong(0, ==, r->configuration_uncommitted_index);

	//add one server
	int ret = raft_add(r, &g_raft_change_req[0], 4, "test_server_1", raft_change_cb_mock);
	munit_assert_int(ret, ==, 0);

	//check
	munit_assert_llong(2, ==, logLastIndex(&r->log));
	munit_assert_llong(2, ==, r->configuration_uncommitted_index);

	//try to add another server but return RAFT_CANTCHANGE
	ret = raft_add(r, &g_raft_change_req[1], 5, "test_server_2", raft_change_cb_mock);
	munit_assert_int(ret, ==, RAFT_CANTCHANGE);

	//wait for conf-change applied
	CLUSTER_STEP_UNTIL_APPLIED(0, 2, 100);

	//then try add again and success
	ret = raft_add(r, &g_raft_change_req[1], 5, "test_server_2", raft_change_cb_mock);
	munit_assert_int(ret, ==, 0);

	return MUNIT_OK;
}
