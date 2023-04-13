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

// static char *cluster_5[] = {"5", NULL};
// static MunitParameterEnum cluster_5_params[] = {
// 	{CLUSTER_N_PARAM, cluster_5},
// 	{NULL, NULL},
// };


// static bool configurationEquals(struct raft_configuration *c1,
//     struct raft_configuration *c2)
// {
//     if (c1->n != c2->n)
//         return false;

//     for (unsigned int i = 0; i < c1->n; i++) {
//         unsigned int j = 0;
//         for (; j < c2->n; j++) {
//             if (c2->servers[j].id == c1->servers[i].id)
//                 break;
//         }
//         if (j == c2->n)
//             return false;
//     }

//     return true;
// }

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
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 2000);
    CLUSTER_STEP_UNTIL_STATE_IS(1, RAFT_LEADER, 2000);

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

// /* 3 replica -> 3 replica, current leader and new replica both fail in joint phase */
// TEST(raft_change, bothCrash, setUp, tearDown, 0, NULL)
// {
//     struct fixture *f = data;
//     struct raft_server servers[3];

//     /* get new configuration */
//     JOINT_NEW_3NODES_WITHOUT_OLD_LEADER(servers);

//     /* current leader receive the new configuration */
//     CLUSTER_NEW_CONFIGURATION(servers, 3);

//     /* apply log index of entering catchup*/
//     CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 2, 1000);
//     JOINT_ASSERT_PHASE(0, RAFT_CONF_CATCHUP);

//     /* enter phase joint */
//     CLUSTER_CHANGE;
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_JOINT, 1000);

//     /* leader and the new replica are failed */
//     CLUSTER_KILL_LEADER;
//     CLUSTER_KILL(3);

//     /* step until old leader back to follower */
//     CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);
//     CLUSTER_STEP_UNTIL_STATE_IS(0, 1, 10000);

//     /* make sure the replica in joint become leader */
//     CLUSTER_ELECT(1);
//     JOINT_ASSERT_PHASE(1, RAFT_CONF_JOINT);

//     return MUNIT_OK;
// }

// /* configuration roll back */
// TEST(raft_change, rollbackToNormal, setUp, tearDown, 0, cluster_5_params)
// {
//     struct fixture *f = data;
//     struct raft_server servers[7];
//     struct raft_apply apply;
//     /* get new configuration with 7 servers */
//     JOINT_NEW_7NODES_WITH_OLD_LEADER(servers);

//     /* disconnect 3 servers from leader */
//     CLUSTER_SATURATE_BOTHWAYS(0, 2);
//     CLUSTER_SATURATE_BOTHWAYS(0, 3);
//     CLUSTER_SATURATE_BOTHWAYS(0, 4);
//     /* current leader receive the new configuration */
//     CLUSTER_NEW_CONFIGURATION(servers, 7);

//     /* step until 0 and 1 has new configuration */
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_CATCHUP, 10000);

//     /* kill leader */
//     CLUSTER_KILL_LEADER;
//     CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);
//     CLUSTER_STEP_UNTIL_STATE_IS(0, 1, 10000);

//     /* server 2 does not have new configuration */
//     CLUSTER_ELECT(2);
//     CLUSTER_APPLY_ADD_X(2, &apply, 1, NULL);

//     /* server 1 rolls back to normal */
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_NORMAL, 10000);

//     return MUNIT_OK;
// }

// /* configuration roll back */
// TEST(raft_change, rollbackToCatchUp, setUp, tearDown, 0, cluster_5_params)
// {
//     struct fixture *f = data;
//     struct raft_server servers[7];
//     struct raft_apply apply;
//     /* get new configuration with 7 servers */
//     JOINT_NEW_7NODES_WITH_OLD_LEADER(servers);

//     /* current leader receive the new configuration */
//     CLUSTER_NEW_CONFIGURATION(servers, 7);

//     CLUSTER_STEP_UNTIL_ALL_APPLIED(2, 1000);
//     /* disconnect 3 servers from leader */
//     CLUSTER_SATURATE_BOTHWAYS(0, 2);
//     CLUSTER_SATURATE_BOTHWAYS(0, 3);
//     CLUSTER_SATURATE_BOTHWAYS(0, 4);

//     CLUSTER_CHANGE;

//     /* step until 0 and 1 in joint */
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_JOINT, 10000);

//     /* kill leader */
//     CLUSTER_KILL_LEADER;
//     CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);
//     CLUSTER_STEP_UNTIL_STATE_IS(0, 1, 10000);

//     /* server 2 does not have new configuration */
//     CLUSTER_ELECT(2);
//     CLUSTER_APPLY_ADD_X(2, &apply, 1, NULL);

//     /* server 1 rolls back to normal */
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_CATCHUP, 10000);

//     return MUNIT_OK;
// }

// /* configuration roll back */
// TEST(raft_change, rollbackToJoint, setUp, tearDown, 0, cluster_5_params)
// {
//     struct fixture *f = data;
//     struct raft_server servers[7];
//     struct raft_apply apply;
//     /* get new configuration with 7 servers */
//     JOINT_NEW_7NODES_WITH_OLD_LEADER(servers);

//     /* current leader receive the new configuration */
//     CLUSTER_NEW_CONFIGURATION(servers, 7);
//     CLUSTER_STEP_UNTIL_ALL_APPLIED(2, 1000);
//     CLUSTER_CHANGE;

//     /* step until 0 and 1 has new configuration */
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_NORMAL, 10000);

//     /* disconnect 3 servers from leader */
//     CLUSTER_SATURATE_BOTHWAYS(0, 2);
//     CLUSTER_SATURATE_BOTHWAYS(0, 3);
//     CLUSTER_SATURATE_BOTHWAYS(0, 4);
//     CLUSTER_SATURATE_BOTHWAYS(0, 5);
//     CLUSTER_SATURATE_BOTHWAYS(0, 6);

//     JOINT_ASSERT_PHASE(1, RAFT_CONF_NORMAL);
//     JOINT_ASSERT_PHASE(5, RAFT_CONF_JOINT);
//     /* kill leader */
//     CLUSTER_KILL_LEADER;

//     /* FIXME: server 5 become leader during server 0 converting to follower*/
//     CLUSTER_STEP_UNTIL_STATE_IS(0, 1, 2000);
//     // CLUSTER_STEP_UNTIL_HAS_NO_LEADER(1000);

//     CLUSTER_APPLY_ADD_X(5, &apply, 1, NULL);

//     /* server 1 rolls back to normal */
//     CLUSTER_STEP_UNTIL_PHASE(1, RAFT_CONF_JOINT, 10000);

//     return MUNIT_OK;
// }

// /* remove some servers from the configuration */
// TEST(raft_change, remove, setUp, tearDown, 0, cluster_5_params)
// {
//     struct fixture *f = data;
//     struct raft_server servers[3];

//     /* get new configuration with 3 servers */
//     for (int i = 0; i < 3; i++) {
//         servers[i].id = f->cluster.servers[i].id;
//     }

//     /* current leader receive the new configuration */
//     CLUSTER_NEW_CONFIGURATION(servers, 3);
//     CLUSTER_STEP_UNTIL_ALL_APPLIED(2, 1000);
//     CLUSTER_CHANGE;
//     CLUSTER_STEP_UNTIL_APPLIED(0, 4, 1000);

//     CONFIGURATION_EQUALS_ALL(servers, 3);
//     return MUNIT_OK;
// }

// TEST(raft_change, change, setUp, tearDown, 0, NULL)
// {
//     struct fixture *f = data;
//     struct raft_server servers[3];

//     /* get new configuration with 3 servers */
//     for (int i = 0; i < 3; i++) {
//         GROW;
//         servers[i].id = f->cluster.servers[i+3].id;
//     }

//     /* current leader receive the new configuration */
//     CLUSTER_NEW_CONFIGURATION(servers, 3);
//     CLUSTER_STEP_UNTIL_ALL_APPLIED(2, 1000);
//     CLUSTER_CHANGE;
//     CLUSTER_STEP_UNTIL_APPLIED(0, 4, 1000);

//     CONFIGURATION_EQUALS_ALL(servers, 3);
//     return MUNIT_OK;
// }

// /* test for a serial of configuration changes
// * 1. joint;
// * 2. add;
// * 3. joint;
// * 4. remove;
// */
// TEST(raft_change, jointAndSingle, setUp, tearDown, 0, NULL)
// {
//     struct fixture *f = data;
//     struct raft_server servers[3];

//     /* joint */
//     JOINT_NEW_3NODES_WITH_OLD_LEADER(servers);
//     CLUSTER_NEW_CONFIGURATION(servers, 3);
//     CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 2, 1000);            // log catchup = 2
//     JOINT_ASSERT_PHASE(0, RAFT_CONF_CATCHUP);
//     CLUSTER_CHANGE;
//     CLUSTER_STEP_UNTIL_APPLIED(3, 4, 1000);                         // log joint = 3, log Cnew = 4
//     CONFIGURATION_EQUALS_ALL(servers, 3);

//     /* add */
//     CLUSTER_ADD(&f->req);
//     CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 5, 2000);            // log add = 5
//     CLUSTER_ASSIGN(&f->req, RAFT_VOTER);
//     CLUSTER_STEP_UNTIL_APPLIED(4, 6, 2000);                         // log assign = 6
//     struct raft *raft = CLUSTER_RAFT(CLUSTER_LEADER);
//     struct raft_server *server = &raft->configuration.servers[3];
//     munit_assert_int(server->role, ==, RAFT_VOTER);

//     /* joint */
//     for (int i = 0; i < 3; i++) {
//         servers[i].id = f->cluster.servers[i].id;
//     }
//     CLUSTER_NEW_CONFIGURATION(servers, 3);
//     CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 7, 1000);            // log catchup = 7
//     JOINT_ASSERT_PHASE(0, RAFT_CONF_CATCHUP);
//     CLUSTER_CHANGE;
//     CLUSTER_STEP_UNTIL_APPLIED(2, 9, 1000);                         // log joint = 8, Cnew = 9
//     CONFIGURATION_EQUALS_ALL(servers, 3);

//     /* remove */
//     raft = CLUSTER_RAFT(CLUSTER_LEADER);
//     raft_remove(raft, &f->req, 3, NULL);
//     CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 10, 1000);
//     struct raft_server test_servers[2];
//     test_servers[0].id = 1;
//     test_servers[1].id = 2;
//     CONFIGURATION_EQUALS_ALL(test_servers, 2);

//     return MUNIT_OK;
// }