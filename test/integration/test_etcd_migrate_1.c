#include "../../src/configuration.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../../src/convert.h"
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

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	SETUP_CLUSTER(2);
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


static char *cluster_2[] = {"2", NULL};
static char *voting_1[] = {"1", NULL};
static MunitParameterEnum cluster_2_with_1_standby_params[] = {
	{CLUSTER_N_PARAM, cluster_2},
	{CLUSTER_N_VOTING_PARAM, voting_1},
	{NULL, NULL},
};

static char *cluster_1[] = {"1", NULL};
static MunitParameterEnum cluster_1_params[] = {
	{CLUSTER_N_PARAM, cluster_1},
	{NULL, NULL},
};

static MunitParameterEnum cluster_2_params[] = {
	{CLUSTER_N_PARAM, cluster_2},
	{NULL, NULL},
};

static char *cluster_3[] = {"3", NULL};
static MunitParameterEnum cluster_3_params[] = {
	{CLUSTER_N_PARAM, cluster_3},
	{NULL, NULL},
};

static char *cluster_4[] = {"4", NULL};
static MunitParameterEnum cluster_4_params[] = {
	{CLUSTER_N_PARAM, cluster_4},
	{NULL, NULL},
};

static char *cluster_5[] = {"5", NULL};
static MunitParameterEnum cluster_5_params[] = {
	{CLUSTER_N_PARAM, cluster_5},
	{NULL, NULL},
};


/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the I'th server is in follower state. */
#define ASSERT_FOLLOWER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_FOLLOWER)

/* Assert that the I'th server is in candidate state. */
#define ASSERT_CANDIDATE(I) \
munit_assert_int(CLUSTER_STATE(I), ==, RAFT_CANDIDATE)

/* Assert that the I'th server is in leader state. */
#define ASSERT_LEADER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_LEADER)

/* Assert that the I'th server is unavailable. */
#define ASSERT_UNAVAILABLE(I) \
munit_assert_int(CLUSTER_STATE(I), ==, RAFT_UNAVAILABLE)

/* Assert that the I'th server has voted for the server with the given ID. */
#define ASSERT_VOTED_FOR(I, ID) munit_assert_int(CLUSTER_VOTED_FOR(I), ==, ID)

/* Assert that the I'th server has the given current term. */
#define ASSERT_TERM(I, TERM)                             \
{                                                    \
struct raft *raft_ = CLUSTER_RAFT(I);            \
munit_assert_int(raft_->current_term, ==, TERM); \
}

/* Assert that the fixture time matches the given value */
#define ASSERT_TIME(TIME) munit_assert_int(CLUSTER_TIME, ==, TIME)

struct result
{
	int status;
	bool done;
};

/* Add a an empty server to the cluster and start it. */
#define GROW                                \
{                                       \
int rv__;                           \
CLUSTER_GROW;                       \
rv__ = raft_start(CLUSTER_RAFT(2)); \
munit_assert_int(rv__, ==, 0);      \
}

static void changeCbAssertResult(struct raft_change *req, int status)
{
	struct result *result = req->data;
	munit_assert_int(status, ==, result->status);
	result->done = true;
}

static bool changeCbHasFired(struct raft_fixture *f, void *arg)
{
	struct result *result = arg;
	(void)f;
	return result->done;
}

		/* Submit an add request. */
#define ADD_SUBMIT(I, ID)                                                     \
struct raft_change _req;                                                  \
char _address[16];                                                        \
struct result _result = {0, false};                                       \
int _rv;                                                                  \
_req.data = &_result;                                                     \
sprintf(_address, "%d", ID);                                              \
_rv =                                                                     \
raft_add(CLUSTER_RAFT(I), &_req, ID, _address, changeCbAssertResult); \
munit_assert_int(_rv, ==, 0);

#define ADD(I, ID)                                            \
do {                                                      \
ADD_SUBMIT(I, ID);                                    \
CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 2000); \
} while (0)

		/* Submit an assign role request. */
#define ASSIGN_SUBMIT(I, ID, ROLE)                                             \
struct raft_change _req;                                                   \
struct result _result = {0, false};                                        \
int _rv;                                                                   \
_req.data = &_result;                                                      \
_rv = raft_assign(CLUSTER_RAFT(I), &_req, ID, ROLE, changeCbAssertResult); \
munit_assert_int(_rv, ==, 0);

		/* Expect the request callback to fire with the given status. */
#define ASSIGN_EXPECT(STATUS) _result.status = STATUS;

		/* Wait until a promote request completes. */
#define ASSIGN_WAIT CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 10000)

		/* Submit a request to assign the I'th server to the given role and wait for the
		 * operation to succeed. */
#define ASSIGN(I, ID, ROLE)         \
do {                            \
ASSIGN_SUBMIT(I, ID, ROLE); \
ASSIGN_WAIT;                \
} while (0)

		/* Invoke raft_assign() against the I'th server and assert it the given error
		 * code. */
#define ASSIGN_ERROR(I, ID, ROLE, RV, ERRMSG)                        \
{                                                                \
struct raft_change __req;                                    \
int __rv;                                                    \
__rv = raft_assign(CLUSTER_RAFT(I), &__req, ID, ROLE, NULL); \
munit_assert_int(__rv, ==, RV);                              \
munit_assert_string_equal(ERRMSG, CLUSTER_ERRMSG(I));        \
}


static void applyCbAssertResult(struct raft_apply *req, int status, void *_)
{
	struct result *result = req->data;
	(void)_;
	munit_assert_int(status, ==, result->status);
	result->done = true;
}

static bool applyCbHasFired(struct raft_fixture *f, void *arg)
{
	struct result *result = arg;
	(void)f;
	return result->done;
}

/* Submit an apply request. */
#define APPLY_SUBMIT(I)                                                      \
struct raft_buffer _buf;                                                 \
struct raft_apply _req;                                                  \
struct result _result = {0, false};                                      \
int _rv;                                                                 \
FsmEncodeSetX(123, &_buf);                                               \
_req.data = &_result;                                                    \
_rv = raft_apply(CLUSTER_RAFT(I), &_req, &_buf, 1, applyCbAssertResult); \
munit_assert_int(_rv, ==, 0);

				/* Expect the apply callback to fire with the given status. */
#define APPLY_EXPECT(STATUS) _result.status = STATUS

				/* Wait until an apply request completes. */
#define APPLY_WAIT CLUSTER_STEP_UNTIL(applyCbHasFired, &_result, 2000)

				/* Submit to the I'th server a request to apply a new RAFT_COMMAND entry and
				 * wait for the operation to succeed. */
#define APPLY(I)         \
do {                 \
APPLY_SUBMIT(I); \
APPLY_WAIT;      \
} while (0)

				/* Submit to the I'th server a request to apply a new RAFT_COMMAND entry and
				 * assert that the given error is returned. */
#define APPLY_ERROR(I, RV, ERRMSG)                                \
do {                                                          \
struct raft_buffer _buf;                                  \
struct raft_apply _req;                                   \
int _rv;                                                  \
FsmEncodeSetX(123, &_buf);                                \
_rv = raft_apply(CLUSTER_RAFT(I), &_req, &_buf, 1, NULL); \
munit_assert_int(_rv, ==, RV);                            \
munit_assert_string_equal(CLUSTER_ERRMSG(I), ERRMSG);     \
raft_free(_buf.base);                                     \
} while (0)


SUITE(etcd_migrate)

TEST(etcd_migrate,
     learnerPromotion,
     setUp,
     tearDown,
     0,
     cluster_2_with_1_standby_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_START;
	CLUSTER_STEP_UNTIL_HAS_LEADER(2000);
	ASSERT_LEADER(0);
	ASSERT_FOLLOWER(1);

	/* Convert server 1 to voter */
	ASSIGN(0, 2, RAFT_VOTER);
	CLUSTER_STEP_UNTIL_APPLIED(1, 2, 2000);

	/* Set smaller random election timeout for server 1*/
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 2000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1000);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_CANDIDATE, 2000);
	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_STEP_UNTIL_HAS_LEADER(2000);

	ASSERT_FOLLOWER(0);
	ASSERT_LEADER(1);

	return MUNIT_OK;
}

TEST(etcd_migrate, candidateConcede, setUp, tearDown, 0, cluster_3_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 2000);
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_CANDIDATE, 2000);
	ASSERT_TERM(1, 2);

	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 2000);
	ASSERT_TERM(1, 2);

	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_DESATURATE_BOTHWAYS(0, 2);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 1000);
	ASSERT_TERM(0, 2);

	return MUNIT_OK;
}

TEST(etcd_migrate, singleNodeCandidate, setUp, tearDown, 0, cluster_1_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_START;
	ASSERT_LEADER(0);

	return MUNIT_OK;
}

TEST(etcd_migrate, singleNodePreCandidate, setUp, tearDown, 0, cluster_1_params)
{
	struct fixture *f = data;
	(void)params;

	raft_set_pre_vote(CLUSTER_RAFT(0), true);
	CLUSTER_START;
	ASSERT_LEADER(0);

	return MUNIT_OK;
}

TEST(etcd_migrate, proposalCluster3With3NodesUp, setUp, tearDown, 0, cluster_3_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	APPLY(0);
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 1));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 2));

	return MUNIT_OK;
}

TEST(etcd_migrate, proposalCluster3With2NodesUp, setUp, tearDown, 0, cluster_3_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	APPLY(0);
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 1));
	munit_assert(!raft_fixture_log_cmp(&f->cluster, 0, 2));

	return MUNIT_OK;
}

TEST(etcd_migrate, proposalCluster3With1NodesUp, setUp, tearDown, 0, cluster_3_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);

	CLUSTER_STEP;

	APPLY_ERROR(0, RAFT_NOTLEADER, CLUSTER_ERRMSG(0));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 1));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 2));

	return MUNIT_OK;
}

TEST(etcd_migrate, proposalCluster4With2NodesUp, setUp, tearDown, 0, cluster_4_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_SATURATE_BOTHWAYS(0, 3);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	CLUSTER_SATURATE_BOTHWAYS(1, 3);
	CLUSTER_SATURATE_BOTHWAYS(2, 3);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);

	CLUSTER_STEP;

	APPLY_ERROR(0, RAFT_NOTLEADER, CLUSTER_ERRMSG(0));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 1));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 2));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 3));

	return MUNIT_OK;
}

TEST(etcd_migrate, proposalCluster5With2NodesUp, setUp, tearDown, 0, cluster_5_params)
{
	struct fixture *f = data;
	(void)params;

	CLUSTER_SATURATE_BOTHWAYS(0, 3);
	CLUSTER_SATURATE_BOTHWAYS(0, 4);
	CLUSTER_SATURATE_BOTHWAYS(1, 3);
	CLUSTER_SATURATE_BOTHWAYS(1, 4);
	CLUSTER_SATURATE_BOTHWAYS(2, 3);
	CLUSTER_SATURATE_BOTHWAYS(2, 4);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	APPLY(0);
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 1));
	munit_assert(raft_fixture_log_cmp(&f->cluster, 0, 2));
	munit_assert(!raft_fixture_log_cmp(&f->cluster, 0, 3));
	munit_assert(!raft_fixture_log_cmp(&f->cluster, 0, 4));

	return MUNIT_OK;
}

TEST(etcd_migrate, handleMsgAppPreviousNonExist, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;
	struct raft_apply apply = {0};
	struct raft_append_entries_result ae_result = {
		.last_log_index = 1,
		.rejected = 2,
		.term = 4
	};

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SET_TERM(0, 3);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	CLUSTER_APPLY_ADD_X(0, &apply, 1, NULL);
	CLUSTER_STEP_UNTIL_APPLIED(0, 2, 1000);

	CLUSTER_RAFT(0)->leader_state.progress[1].next_index = 3;

	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_STEP_UNTIL_AE_RES(1, 0, &ae_result, 1000);

	return MUNIT_OK;
}

TEST(etcd_migrate, handleMsgAppPreviousMismatch, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;
	struct raft_apply apply = {0};
	struct raft_entry entry = {.term = 3, .type = RAFT_COMMAND};
	struct raft_append_entries_result ae_result = {
		.last_log_index = 2,
		.rejected = 2,
		.term = 4
	};

	FsmEncodeSetX(0, &entry.buf);
	CLUSTER_ADD_ENTRY(1, &entry);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SET_TERM(0, 3);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	CLUSTER_APPLY_ADD_X(0, &apply, 1, NULL);
	CLUSTER_STEP_UNTIL_APPLIED(0, 2, 1000);

	CLUSTER_RAFT(0)->leader_state.progress[1].next_index = 3;

	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_STEP_UNTIL_AE_RES(1, 0, &ae_result, 1000);

	return MUNIT_OK;
}

TEST(etcd_migrate, handleMsgAppLogTermConflicts, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;
	struct raft_apply apply = {0};
	struct raft_entry entry = {.term = 3, .type = RAFT_COMMAND};
	struct raft_append_entries_result ae_result = {
		.last_log_index = 2,
		.rejected = 0,
		.term = 4
	};

	FsmEncodeSetX(0, &entry.buf);
	CLUSTER_ADD_ENTRY(1, &entry);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SET_TERM(0, 3);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	CLUSTER_APPLY_ADD_X(0, &apply, 1, NULL);
	CLUSTER_STEP_UNTIL_APPLIED(0, 2, 1000);

	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_STEP_UNTIL_AE_RES(1, 0, &ae_result, 1000);

	return MUNIT_OK;
}

TEST(etcd_migrate, handleMsgAppUpdateCommitIndex, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;
	struct raft_apply apply = {0};

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SET_TERM(0, 3);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

	CLUSTER_APPLY_ADD_X(0, &apply, 1, NULL);
	CLUSTER_STEP_UNTIL_APPLIED(0, 2, 1000);

	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 1);

	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_STEP_UNTIL_ELAPSED(1000);

	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 2);

	return MUNIT_OK;
}

TEST(etcd_migrate, handleHeartbeatIncreaseCommitIndex, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct raft_apply apply = {0};
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 1);

	CLUSTER_APPLY_ADD_X(0, &apply, 1, NULL);
	CLUSTER_STEP_UNTIL_COMMITTED(0, 2, 2000);

	CLUSTER_STEP_UNTIL_ELAPSED(200);
	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 2);

	return MUNIT_OK;
}

TEST(etcd_migrate, handleHeartbeatIgnoreDecreaseCommitIndex, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct raft_apply apply = {0};
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 1);

	CLUSTER_APPLY_ADD_X(0, &apply, 1, NULL);
	CLUSTER_STEP_UNTIL_COMMITTED(0, 2, 2000);

	CLUSTER_STEP_UNTIL_ELAPSED(200);
	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 2);

	// mock decreased commit index
	struct raft_append_entries ae = {
		.term =  2,
		.n_entries = 0,
		.prev_log_index = 2,
		.prev_log_term = 2,
		.leader_commit = 1
	};
	CLUSTER_STEP_HEARTBEAT_MOCK(&f->cluster, 0, 1, &ae);

	struct raft_append_entries_result ae_result = {
		.last_log_index = 2,
		.rejected = 0,
		.term = 2
	};
	CLUSTER_STEP_UNTIL_AE_RES(1, 0, &ae_result, 1000);

	munit_assert_int64(CLUSTER_RAFT(1)->commit_index, ==, 2);

	return MUNIT_OK;
}

TEST(etcd_migrate, recvMsgVoteWithLowerLogTerm, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct raft_entry entry = {.type = RAFT_COMMAND, .term = 3};
	struct fixture *f = data;

	FsmEncodeSetX(0, &entry.buf);
	CLUSTER_ADD_ENTRY(1, &entry);

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);

	struct raft_request_vote rv = {
		.term = 2,
		.last_log_term = 1,
		.last_log_index = 1,
		.candidate_id = 0
	};
	CLUSTER_STEP_UNTIL_RV(1, &rv, 1100);

	struct raft_request_vote_result res = {
		.term = 2,
		.vote_granted = false
	};
	CLUSTER_STEP_UNTIL_RV_RES(1, 0, &res, 100);

	return MUNIT_OK;
}

TEST(etcd_migrate, recvMsgVoteWithLowerLogIndex, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct raft_entry entry = {.type = RAFT_COMMAND, .term = 1};
	struct fixture *f = data;

	FsmEncodeSetX(0, &entry.buf);
	CLUSTER_ADD_ENTRY(1, &entry);

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);

	struct raft_request_vote rv = {
		.term = 2,
		.last_log_term = 1,
		.last_log_index = 1,
		.candidate_id = 0
	};
	CLUSTER_STEP_UNTIL_RV(1, &rv, 1100);

	struct raft_request_vote_result res = {
		.term = 2,
		.vote_granted = false
	};
	CLUSTER_STEP_UNTIL_RV_RES(1, 0, &res, 100);

	return MUNIT_OK;
}

TEST(etcd_migrate, recvMsgVoteWithHigherLogTerm, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct raft_entry entry = {.type = RAFT_COMMAND, .term = 1};
	struct raft_entry entry_leader = {.type = RAFT_COMMAND, .term = 2};
	struct fixture *f = data;

	FsmEncodeSetX(0, &entry.buf);
	CLUSTER_ADD_ENTRY(1, &entry);

	FsmEncodeSetX(0, &entry_leader.buf);
	CLUSTER_ADD_ENTRY(0, &entry_leader);

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);

	struct raft_request_vote rv = {
		.term = 2,
		.last_log_term = 2,
		.last_log_index = 2,
		.candidate_id = 0
	};
	CLUSTER_STEP_UNTIL_RV(1, &rv, 1100);

	struct raft_request_vote_result res = {
		.term = 2,
		.vote_granted = true
	};
	CLUSTER_STEP_UNTIL_RV_RES(1, 0, &res, 100);

	return MUNIT_OK;
}

TEST(etcd_migrate, recvMsgVoteWithHigherLogIndex, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct raft_entry entry_leader = {.type = RAFT_COMMAND, .term = 1};
	struct fixture *f = data;

	FsmEncodeSetX(0, &entry_leader.buf);
	CLUSTER_ADD_ENTRY(0, &entry_leader);

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);

	struct raft_request_vote rv = {
		.term = 2,
		.last_log_term = 1,
		.last_log_index = 2,
		.candidate_id = 0
	};
	CLUSTER_STEP_UNTIL_RV(1, &rv, 1100);

	struct raft_request_vote_result res = {
		.term = 2,
		.vote_granted = true
	};
	CLUSTER_STEP_UNTIL_RV_RES(1, 0, &res, 100);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionUnavailableToFollower, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_UNAVAILABLE);
	convertToFollower(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionFollowerToCandidate, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);
	convertToCandidate(CLUSTER_RAFT(0), false);

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_CANDIDATE);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionCandidateToFollower, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 1010);

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_CANDIDATE);
	convertToFollower(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionCandidateToLeader, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 1010);

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_CANDIDATE);
	convertToLeader(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_LEADER);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionLeaderToFollower, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_LEADER);
	convertToFollower(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionFollowerToUnavailable, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);
	convertToUnavailable(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_UNAVAILABLE);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionCandidateToUnavailable, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 1010);

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_CANDIDATE);
	convertToUnavailable(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_UNAVAILABLE);

	return MUNIT_OK;
}

TEST(etcd_migrate, stateTransitionLeaderToUnavailable, setUp, tearDown, 0, cluster_2_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_LEADER);
	convertToUnavailable(CLUSTER_RAFT(0));

	munit_assert_ushort(CLUSTER_RAFT(0)->state, ==, RAFT_UNAVAILABLE);

	return MUNIT_OK;
}

TEST(etcd_migrate, allServerStepdownFollowerToFollower, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 2000);
	convertToFollower(CLUSTER_RAFT(0));

	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 2000);

	convertToFollower(CLUSTER_RAFT(2));
	CLUSTER_DESATURATE_BOTHWAYS(1, 2);

	CLUSTER_STEP_UNTIL_ELAPSED(200);

	munit_assert_int64(CLUSTER_RAFT(2)->current_term, == , 3);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, allServerStepdownCandidateToFollower, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 2000);
	convertToFollower(CLUSTER_RAFT(0));

	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 2000);

	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_CANDIDATE, 1100);
	CLUSTER_DESATURATE_BOTHWAYS(1, 2);

	CLUSTER_STEP_UNTIL_ELAPSED(200);

	munit_assert_int64(CLUSTER_RAFT(2)->current_term, == , 3);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, allServerStepdownLeaderToFollower, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 2000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 1100);
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 1500);

	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_STEP_UNTIL_ELAPSED(200);

	munit_assert_int64(CLUSTER_RAFT(0)->current_term, == , 3);
	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, leaderStepdownWhenQuorumActive, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	struct raft_append_entries_result res = {.term = 2, .rejected = 0, .last_log_index = 1};
	CLUSTER_STEP_UNTIL_AE_RES(1, 0, &res, 1000);
	CLUSTER_STEP_UNTIL_AE_RES(2, 0, &res, 1000);

	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_LEADER);

	return MUNIT_OK;
}

TEST(etcd_migrate, leaderStepdownWhenQuorumLost, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1100);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);

	CLUSTER_STEP_UNTIL_ELAPSED(1100);

	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, leaderSupersedingWithCheckQuorum, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 2000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);
	CLUSTER_START;
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 1500);
	munit_assert_uint16(CLUSTER_RAFT(1)->state, ==, RAFT_LEADER);

	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);

	CLUSTER_SATURATE_BOTHWAYS(1, 0);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);

	CLUSTER_STEP_UNTIL_ELAPSED(2000);
	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_CANDIDATE);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);

	convertToCandidate(CLUSTER_RAFT(2), false);
	electionResetTimer(CLUSTER_RAFT(2));

	CLUSTER_STEP_UNTIL_ELAPSED(1100);
	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_LEADER);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);

	return MUNIT_OK;
}

TEST(etcd_migrate, freeStuckCandidateWithCheckQuorum, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;

	CLUSTER_START;
	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1500);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1500);

	munit_assert_uint16(CLUSTER_RAFT(1)->state, ==, RAFT_FOLLOWER);

	CLUSTER_SATURATE_BOTHWAYS(0, 1);
	CLUSTER_SATURATE_BOTHWAYS(0, 2);

	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 2000);
	CLUSTER_DESATURATE_BOTHWAYS(0, 1);
	CLUSTER_STEP_UNTIL_ELAPSED(200);
	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);
	munit_assert_int64(CLUSTER_RAFT(0)->current_term, ==, CLUSTER_RAFT(1)->current_term);

	return MUNIT_OK;
}

TEST(etcd_migrate, disruptiveFollower, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;
	raft_term term;

	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1100);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);
	raft_set_pre_vote(CLUSTER_RAFT(0), false);
	raft_set_pre_vote(CLUSTER_RAFT(1), false);
	raft_set_pre_vote(CLUSTER_RAFT(2), false);
	CLUSTER_START;
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 1500);

	term = CLUSTER_RAFT(0)->current_term;
	munit_assert_uint16(CLUSTER_RAFT(1)->state, ==, RAFT_FOLLOWER);

	CLUSTER_SET_NETWORK_LATENCY(0, 1200);
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_CANDIDATE, 2000);
	CLUSTER_STEP_UNTIL_ELAPSED(200);

	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_FOLLOWER);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);
	munit_assert_int64(CLUSTER_RAFT(0)->current_term, ==, term + 1);
	munit_assert_int64(CLUSTER_RAFT(0)->current_term, ==, CLUSTER_RAFT(1)->current_term);

	return MUNIT_OK;
}

TEST(etcd_migrate, disruptiveFollowerPreVote, setUp, tearDown, 0, cluster_3_params)
{
	(void)params;
	struct fixture *f = data;
	raft_term term;

	raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 1500);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1000);
	raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 2000);
	raft_set_pre_vote(CLUSTER_RAFT(0), true);
	raft_set_pre_vote(CLUSTER_RAFT(1), true);
	raft_set_pre_vote(CLUSTER_RAFT(2), true);
	CLUSTER_START;

	CLUSTER_SATURATE_BOTHWAYS(1, 0);
	CLUSTER_SATURATE_BOTHWAYS(1, 2);
	CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);
	CLUSTER_DESATURATE_BOTHWAYS(1, 0);
	CLUSTER_DESATURATE_BOTHWAYS(1, 2);

	munit_assert_uint16(CLUSTER_RAFT(1)->state, ==, RAFT_CANDIDATE);
	term = CLUSTER_RAFT(1)->current_term;
	CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_FOLLOWER, 2000);

	munit_assert_uint16(CLUSTER_RAFT(0)->state, ==, RAFT_LEADER);
	munit_assert_uint16(CLUSTER_RAFT(2)->state, ==, RAFT_FOLLOWER);
	munit_assert_int64(CLUSTER_RAFT(1)->current_term, ==, term + 1);
	munit_assert_int64(CLUSTER_RAFT(1)->current_term, ==, CLUSTER_RAFT(0)->current_term);

	return MUNIT_OK;
}



