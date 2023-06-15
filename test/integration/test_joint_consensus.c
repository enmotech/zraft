#include "../lib/cluster.h"
#include "../lib/runner.h"

#define CONFIGURATION_EQUALS(t_servers, num, i) \
    {\
        struct raft_configuration conf; \
        conf.n = num; \
        conf.servers = t_servers; \
        bool rv = configurationEquals(&conf, &(CLUSTER_RAFT(i)->configuration)); \
        munit_assert(rv); \
    }

#define CONFIGURATION_EQUALS_ALL(servers, num) \
    { \
        for (int i = 0; i < num; i++) {\
            CONFIGURATION_EQUALS(servers, num, servers[i].id - 1); \
        } \
    }

struct fixture
{
	FIXTURE_CLUSTER;
	struct raft_change req;
};

#define GROW \
    { \
        int rv__ = 0; \
        CLUSTER_GROW; \
        rv__ = raft_start(CLUSTER_RAFT(CLUSTER_N - 1)); \
        munit_assert_int(rv__, ==, 0);      \
    }

#define JOINT_NEW_3NODES_WITH_OLD_LEADER(new_servers) \
    { \
        munit_assert_int(CLUSTER_N, ==, 3); \
        int index = CLUSTER_LEADER; \
        for (int i = 0; i < 3; i++) { \
            new_servers[i].id = f->cluster.servers[i].id; \
        } \
        GROW; \
        for (int i = 0; i < 3; i++) { \
            if (i != index) { \
                new_servers[2].id = f->cluster.servers[3].id; \
                break; \
            } \
        } \
    }

#define JOINT_NEW_3NODES_WITHOUT_OLD_LEADER(new_servers) \
    { \
        munit_assert_int(CLUSTER_N, ==, 3); \
        int index = CLUSTER_LEADER; \
        for (int i = 0; i < 3; i++) { \
            new_servers[i].id = f->cluster.servers[i].id; \
        } \
        GROW; \
        for (int i = index; i < 2; i++) { \
            new_servers[i] = new_servers[i+1]; \
        } \
        new_servers[2].id = f->cluster.servers[3].id; \
    }

#define JOINT_NEW_7NODES_WITH_OLD_LEADER(new_servers) \
    { \
        for (int i = f->cluster.n; i < 7; i++) { \
            GROW; \
        } \
        for (int i = 0; i < 7; i++) { \
            new_servers[i].id = f->cluster.servers[i].id; \
        } \
    }

#define JOINT_ASSERT_PHASE(INDEX, PHASE) \
    {\
        munit_assert_int(f->cluster.servers[INDEX].raft.configuration.phase, ==, PHASE); \
    }

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(3);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

static char * enable_recorder[] = {"1", NULL};
static MunitParameterEnum enable_recorder_params[] = {
    {CLUSTER_RECORDER_PARAM, enable_recorder},
    {NULL, NULL},
};

static char * cluster_2[] = {"2", NULL};
static MunitParameterEnum cluster_2_params[] = {
    {CLUSTER_RECORDER_PARAM, enable_recorder},
    {CLUSTER_N_PARAM, cluster_2},
    {NULL, NULL},
};

SUITE(raft_change)

TEST(raft_change, replace_non_leader, setUp, tearDown, 0,
     enable_recorder_params)
{
    struct fixture *f = data;
    raft_index index;
    struct raft *r;
    struct raft_configuration *c;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    CLUSTER_JOINT_PROMOTE(&f->req, 4, 2);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 1, 1000);
    r = CLUSTER_RAFT(0);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);

    CLUSTER_STEP_UNTIL_ELAPSED(1000);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);
    CLUSTER_REMOVE(&f->req, 2);

    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 2, 1000);
    r = CLUSTER_RAFT(0);
    c = &r->configuration;
    munit_assert(c->n == 3);
    munit_assert(c->phase == RAFT_CONF_NORMAL);
    munit_assert(c->servers[0].id == 1);
    munit_assert(c->servers[0].role == RAFT_VOTER);
    munit_assert(c->servers[0].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[1].id == 3);
    munit_assert(c->servers[1].role == RAFT_VOTER);
    munit_assert(c->servers[1].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[2].id == 4);
    munit_assert(c->servers[2].role == RAFT_VOTER);
    munit_assert(c->servers[2].group == RAFT_GROUP_OLD);

    return MUNIT_OK;
}

TEST(raft_change, replace_leader, setUp, tearDown, 0, enable_recorder_params)
{
    struct fixture *f = data;
    raft_index index;
    struct raft *r;
    struct raft_configuration *c;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    CLUSTER_JOINT_PROMOTE(&f->req, 4, 1);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 1, 1000);
    r = CLUSTER_RAFT(0);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);

    CLUSTER_STEP_UNTIL_ELAPSED(1000);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);
    CLUSTER_REMOVE(&f->req, 1);

    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 2, 1000);
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(1000);
    CLUSTER_ELECT(1);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);

    r = CLUSTER_RAFT(1);
    c = &r->configuration;
    munit_assert(c->n == 3);
    munit_assert(c->phase == RAFT_CONF_NORMAL);
    munit_assert(c->servers[0].id == 2);
    munit_assert(c->servers[0].role == RAFT_VOTER);
    munit_assert(c->servers[0].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[1].id == 3);
    munit_assert(c->servers[1].role == RAFT_VOTER);
    munit_assert(c->servers[1].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[2].id == 4);
    munit_assert(c->servers[2].role == RAFT_VOTER);
    munit_assert(c->servers[2].group == RAFT_GROUP_OLD);

    return MUNIT_OK;
}

TEST(raft_change, replace_leader_2, setUp, tearDown, 0, cluster_2_params)
{
    struct fixture *f = data;
    raft_index index;
    struct raft *r;
    struct raft_configuration *c;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    CLUSTER_JOINT_PROMOTE(&f->req, 3, 1);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 1, 1000);
    r = CLUSTER_RAFT(0);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);

    CLUSTER_STEP_UNTIL_ELAPSED(1000);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);
    CLUSTER_REMOVE(&f->req, 1);

    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 2, 1000);
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(1000);
    CLUSTER_ELECT(1);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);

    r = CLUSTER_RAFT(1);
    c = &r->configuration;
    munit_assert(c->n == 2);
    munit_assert(c->phase == RAFT_CONF_NORMAL);
    munit_assert(c->servers[0].id == 2);
    munit_assert(c->servers[0].role == RAFT_VOTER);
    munit_assert(c->servers[0].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[1].id == 3);
    munit_assert(c->servers[1].role == RAFT_VOTER);
    munit_assert(c->servers[1].group == RAFT_GROUP_OLD);

    return MUNIT_OK;
}

TEST(raft_change, crash_two_replica, setUp, tearDown, 0,
     enable_recorder_params)
{
    struct fixture *f = data;
    raft_index index;
    struct raft *r;
    struct raft_configuration *c;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    CLUSTER_JOINT_PROMOTE(&f->req, 4, 1);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index + 1, 1000);
    r = CLUSTER_RAFT(0);
    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);
    munit_assert(c->n ==  4);
    // kill two replica
    CLUSTER_KILL(0);
    CLUSTER_KILL(3);
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(2000);

    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_FOLLOWER, 2000);
    CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 2000);

    CLUSTER_MAKE_PROGRESS;

    c = &r->configuration;
    munit_assert(c->phase == RAFT_CONF_JOINT);
    CLUSTER_REMOVE(&f->req, 1);

    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_COMMITTED(1, index + 3, 3000);
    r = CLUSTER_RAFT(CLUSTER_LEADER);
    c = &r->configuration;
    munit_assert(c->n == 3);
    munit_assert(c->phase == RAFT_CONF_NORMAL);
    munit_assert(c->servers[0].id == 2);
    munit_assert(c->servers[0].role == RAFT_VOTER);
    munit_assert(c->servers[0].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[1].id == 3);
    munit_assert(c->servers[1].role == RAFT_VOTER);
    munit_assert(c->servers[1].group == RAFT_GROUP_OLD);
    munit_assert(c->servers[2].id == 4);
    munit_assert(c->servers[2].role == RAFT_VOTER);
    munit_assert(c->servers[2].group == RAFT_GROUP_OLD);

    return MUNIT_OK;
}

TEST(raft_change, promote_bad_role, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    raft_index index;
    int rv;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    rv = raft_joint_promote(CLUSTER_RAFT(CLUSTER_LEADER), &f->req, 4,
                            RAFT_SPARE, 1, NULL);
    munit_assert_int(rv, ==, RAFT_BADROLE);

    return MUNIT_OK;
}

TEST(raft_change, promote_non_exist, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    raft_index index;
    int rv;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    rv = raft_joint_promote(CLUSTER_RAFT(CLUSTER_LEADER), &f->req, 5,
                            RAFT_VOTER, 1, NULL);
    munit_assert_int(rv, ==, RAFT_NOTFOUND);

    return MUNIT_OK;
}

TEST(raft_change, promote_voter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    raft_index index;
    int rv;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    rv = raft_joint_promote(CLUSTER_RAFT(CLUSTER_LEADER), &f->req, 2,
                            RAFT_VOTER, 1, NULL);
    munit_assert_int(rv, ==, RAFT_BADROLE);

    return MUNIT_OK;
}

TEST(raft_change, promote_remove_non_exist, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    raft_index index;
    int rv;

    CLUSTER_ADD(&f->req);
    munit_assert(CLUSTER_STATE(0) == RAFT_LEADER);
    index = raft_fixture_last_index(&f->cluster, 0);
    CLUSTER_STEP_UNTIL_COMMITTED(0, index, 1000);

    rv = raft_joint_promote(CLUSTER_RAFT(CLUSTER_LEADER), &f->req, 4,
                            RAFT_VOTER, 5, NULL);
    munit_assert_int(rv, ==, RAFT_NOTFOUND);

    return MUNIT_OK;
}
