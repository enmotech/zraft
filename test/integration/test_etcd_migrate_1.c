#include "../../src/configuration.h"
#include "../lib/cluster.h"
#include "../lib/runner.h"

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
