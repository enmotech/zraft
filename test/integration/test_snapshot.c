#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../lib/munit_mock.h"
#include "../../src/snapshot.h"
#include "../../include/raft.h"

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
static void refsIncrTest(struct raft_log *l,
                     const raft_term term,
                     const raft_index index)
{
    size_t key;                  /* Hash table key for the given index. */
    struct raft_entry_ref *slot; /* Slot for the given term/index */


    key = (size_t)((index - 1) % l->refs_size);
    /* Lookup the slot associated with the given term/index, which must have
     * been previously inserted. */
    slot = &l->refs[key];
    while (1) {
        if (slot->term == term) {
            break;
        }
        slot = slot->next;
    }

    slot->count++;
}

static bool refsDecrTest(struct raft_log *l,
                     const raft_term term,
                     const raft_index index)
{
    size_t key;                       /* Hash table key for the given index. */
    struct raft_entry_ref *slot;      /* Slot for the given term/index */

    key = (size_t)((index - 1) % l->refs_size);

    /* Lookup the slot associated with the given term/index, keeping track of
     * its previous slot in the bucket list. */
    slot = &l->refs[key];
    while (1) {
        if (slot->term == term) {
            break;
        }
        slot = slot->next;
    }

    slot->count--;
    return true;
}
/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/
#define ENABLE_CHANGE_AND_FREE_TRAILING                          \
    {                                                            \
        unsigned i;                                              \
        for (i = 0; i < CLUSTER_N; i++) {                        \
            raft_enable_dynamic_trailing(CLUSTER_RAFT(i), true); \
        }                                                        \
    }
/* Set the snapshot threshold on all servers of the cluster */
#define SET_SNAPSHOT_THRESHOLD(VALUE)                            \
    {                                                            \
        unsigned i;                                              \
        for (i = 0; i < CLUSTER_N; i++) {                        \
            raft_set_snapshot_threshold(CLUSTER_RAFT(i), VALUE); \
        }                                                        \
    }

/* Set the snapshot trailing logs number on all servers of the cluster */
#define SET_SNAPSHOT_TRAILING(VALUE)                            \
    {                                                           \
        unsigned i;                                             \
        for (i = 0; i < CLUSTER_N; i++) {                       \
            raft_set_snapshot_trailing(CLUSTER_RAFT(i), VALUE); \
        }                                                       \
    }

/* Set the snapshot timeout on all servers of the cluster */
#define SET_SNAPSHOT_TIMEOUT(VALUE)                                   \
    {                                                                 \
        unsigned i;                                                   \
        for (i = 0; i < CLUSTER_N; i++) {                             \
            raft_set_install_snapshot_timeout(CLUSTER_RAFT(i), VALUE);\
        }                                                             \
    }

/******************************************************************************
 *
 * Successfully install a snapshot
 *
 *****************************************************************************/

SUITE(snapshot)

/* Install a snapshot on a follower that has fallen behind. */
TEST(snapshot, installOne, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect the follower and wait for it to catch up */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 5000);

    /* Check that the leader has sent a snapshot */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    return MUNIT_OK;
}

/* Install snapshot times out and leader retries */
TEST(snapshot, installOneTimeOut, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Apply a few of entries, to force a snapshot to be taken. Drop all network
     * traffic between servers 0 and 2 in order for AppendEntries RPCs to not be
     * replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect both servers and set a high disk latency on server 2 so that
     * the InstallSnapshot RPC will time out */
    CLUSTER_SET_DISK_LATENCY(2, 300);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);

    /* Wait a while and check that the leader has sent a snapshot */
    CLUSTER_STEP_UNTIL_ELAPSED(300);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);

    /* Wait for the snapshot to be installed */
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 5000);

    /* Assert that the leader has retried the InstallSnapshot RPC */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 2);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 2);

    return MUNIT_OK;
}

/* Install snapshot to an offline node */
TEST(snapshot, installOneDisconnectedFromBeginningReconnects, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Apply a few of entries, to force a snapshot to be taken. Disconnect
     * servers 0 and 2 so that the network calls return failure status */
    CLUSTER_DISCONNECT(0, 2);
    CLUSTER_DISCONNECT(2, 0);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Wait a while so leader detects offline node */
    CLUSTER_STEP_UNTIL_ELAPSED(2000);

    /* Assert that the leader doesn't try sending a snapshot to an offline node */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);

    CLUSTER_RECONNECT(0, 2);
    CLUSTER_RECONNECT(2, 0);
    /* Wait for the snapshot to be installed */
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 5000);

    /* Assert that the leader has sent an InstallSnapshot RPC */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);

    return MUNIT_OK;
}

/* Install snapshot to an offline node that went down during operation */
TEST(snapshot, installOneDisconnectedDuringOperationReconnects, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Apply a few of entries */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Wait for follower to catch up*/
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 5000);
    /* Assert that the leader hasn't sent an InstallSnapshot RPC  */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);

    CLUSTER_DISCONNECT(0, 2);
    CLUSTER_DISCONNECT(2, 0);

    /* Wait a while so leader detects offline node */
    CLUSTER_STEP_UNTIL_ELAPSED(2000);

    /* Apply a few more entries */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Assert that the leader doesn't try sending snapshot to an offline node */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);

    CLUSTER_RECONNECT(0, 2);
    CLUSTER_RECONNECT(2, 0);
    CLUSTER_STEP_UNTIL_APPLIED(2, 7, 5000);

    /* Assert that the leader has tried sending an InstallSnapshot RPC */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);

    return MUNIT_OK;
}

/* No snapshots sent to killed nodes */
TEST(snapshot, noSnapshotInstallToKilled, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Kill a server */
    CLUSTER_KILL(2);

    /* Apply a few of entries */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Wait a while */
    CLUSTER_STEP_UNTIL_ELAPSED(4000);

    /* Assert that the leader hasn't sent an InstallSnapshot RPC  */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    return MUNIT_OK;
}

/* Install snapshot times out and leader retries, afterwards AppendEntries resume */
TEST(snapshot, installOneTimeOutAppendAfter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Apply a few of entries, to force a snapshot to be taken. Drop all network
     * traffic between servers 0 and 2 in order for AppendEntries RPCs to not be
     * replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect both servers and set a high disk latency on server 2 so that
     * the InstallSnapshot RPC will time out */
    CLUSTER_SET_DISK_LATENCY(2, 300);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);

    /* Wait for the snapshot to be installed */
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 5000);

    /* Append a few entries and check if they are replicated */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(2, 5, 5000);

    /* Assert that the leader has retried the InstallSnapshot RPC */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 4);

    return MUNIT_OK;
}

/* Install 2 snapshots that both time out and assure the follower catches up */
TEST(snapshot, installMultipleTimeOut, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Apply a few of entries, to force a snapshot to be taken. Drop all network
     * traffic between servers 0 and 2 in order for AppendEntries RPCs to not be
     * replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect both servers and set a high disk latency on server 2 so that
     * the InstallSnapshot RPC will time out */
    CLUSTER_SET_DISK_LATENCY(2, 300);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);

    /* Step until the snapshot times out */
    CLUSTER_STEP_UNTIL_ELAPSED(400);

    /* Apply another few of entries, to force a new snapshot to be taken. Drop
     * all traffic between servers 0 and 2 in order for AppendEntries RPCs to not be
     * replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect the follower */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_APPLIED(2, 7, 5000);

    /* Assert that the leader has sent multiple InstallSnapshot RPCs */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), >=, 2);

    return MUNIT_OK;
}

/* Install 2 snapshots that both time out, launch a few regular AppendEntries
 * and assure the follower catches up */
TEST(snapshot, installMultipleTimeOutAppendAfter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    SET_SNAPSHOT_TIMEOUT(200);

    /* Apply a few of entries, to force a snapshot to be taken. Drop all network
     * traffic between servers 0 and 2 in order for AppendEntries RPCs to not be
     * replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect both servers and set a high disk latency on server 2 so that
     * the InstallSnapshot RPC will time out */
    CLUSTER_SET_DISK_LATENCY(2, 300);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);

    /* Step until the snapshot times out */
    CLUSTER_STEP_UNTIL_ELAPSED(400);

    /* Apply another few of entries, to force a new snapshot to be taken. Drop
     * all traffic between servers 0 and 2 in order for AppendEntries RPCs to not be
     * replicated */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect the follower */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    /* Append a few entries and make sure the follower catches up */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(2, 9, 5000);

    /* Assert that the leader has sent multiple InstallSnapshot RPCs */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), >=, 2);

    return MUNIT_OK;
}

static bool server_installing_snapshot(struct raft_fixture *f, void* data) {
    (void) f;
    const struct raft *r = data;
    return r->snapshot.put.data != NULL && r->last_stored == 0;
}

static bool server_taking_snapshot(struct raft_fixture *f, void* data) {
    (void) f;
    const struct raft *r = data;
    return r->snapshot.put.data != NULL && r->last_stored != 0;
}

static bool server_snapshot_done(struct raft_fixture *f, void *data) {
    (void) f;
    const struct raft *r = data;
    return r->snapshot.put.data == NULL;
}

/* Follower receives HeartBeats during the installation of a snapshot */
TEST(snapshot, installSnapshotHeartBeats, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Set a large disk latency on the follower, this will allow some
     * heartbeats to be sent during the snapshot installation */
    CLUSTER_SET_DISK_LATENCY(1, 2000);

    munit_assert_uint(CLUSTER_N_RECV(1, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);

    /* Step the cluster until server 1 installs a snapshot */
    const struct raft *r = CLUSTER_RAFT(1);
    CLUSTER_DESATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL(server_installing_snapshot, (void*) r, 2000);
    munit_assert_uint(CLUSTER_N_RECV(1, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);

    /* Count the number of AppendEntries RPCs received during the snapshot
     * install*/
    unsigned before = CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES);
    CLUSTER_STEP_UNTIL(server_snapshot_done, (void*) r, 5000);
    unsigned after = CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES);
    munit_assert_uint(before, < , after);

    /* Check that the InstallSnapshot RPC was not resent */
    munit_assert_uint(CLUSTER_N_RECV(1, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);

    /* Check that the snapshot was applied and we can still make progress */
    CLUSTER_STEP_UNTIL_APPLIED(1, 4, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(1, 6, 5000);

    return MUNIT_OK;
}

/* InstallSnapshot RPC arrives while persisting Entries */
TEST(snapshot, installSnapshotDuringEntriesWrite, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set a large disk latency on the follower, this will allow a
     * InstallSnapshot RPC to arrive while the entries are being persisted. */
    CLUSTER_SET_DISK_LATENCY(1, 2000);
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    /* Replicate some entries, these will take a while to persist */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Make sure leader can't succesfully send any more entries */
    CLUSTER_DISCONNECT(0,1);
    CLUSTER_MAKE_PROGRESS; /* Snapshot taken here */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS; /* Snapshot taken here */
    CLUSTER_MAKE_PROGRESS;

    /* Snapshot with index 6 is sent while follower is still writing the entries
     * to disk that arrived before the disconnect. */
    CLUSTER_RECONNECT(0,1);

    /* Make sure follower is up to date */
    CLUSTER_STEP_UNTIL_APPLIED(1, 7, 5000);
    return MUNIT_OK;
}

/* Follower receives AppendEntries RPCs while taking a snapshot */
TEST(snapshot, takeSnapshotAppendEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    /* Set a large disk latency on the follower, this will allow AppendEntries
     * to be sent while a snapshot is taken */
    CLUSTER_SET_DISK_LATENCY(1, 2000);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    /* Step the cluster until server 1 takes a snapshot */
    const struct raft *r = CLUSTER_RAFT(1);
    CLUSTER_STEP_UNTIL(server_taking_snapshot, (void*) r, 2000);

    /* Send AppendEntries RPCs while server 1 is taking a snapshot */
    static struct raft_apply reqs[5];
    for (int i = 0; i < 5; i++) {
        CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &reqs[i], 1, NULL);
    }
    CLUSTER_STEP_UNTIL(server_snapshot_done, (void*) r, 5000);

    /* Make sure the AppendEntries are applied and we can make progress */
    CLUSTER_STEP_UNTIL_APPLIED(1, 9, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(1, 11, 5000);
    return MUNIT_OK;
}

TEST(snapshot, reInstallAfterRejected, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    struct raft_install_snapshot snap = {.term = 4};

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    munit_assert_uint64(CLUSTER_TERM(0), ==, 2);
    /* Set term for server 2 */
    CLUSTER_STEP_UNTIL_TERM_IS(2, 3, 10000);
    munit_assert_uint64(CLUSTER_TERM(2), ==, 3);

    /* Reconnect the follower and wait for it to catch up */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_SNAPSHOT(&f->cluster, 0, 2, &snap, 3000);
    snap.term = 2;
    CLUSTER_STEP_SNAPSHOT_MOCK(&f->cluster, 0, 2, &snap);
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 5000);

    /* Check that the leader has sent a snapshot */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 2);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 2);
    return MUNIT_OK;
}

static int fakeSnapshot(struct raft_fsm *fsm, struct raft_buffer *bufs[],
                        unsigned *n_bufs)
{
    (void)fsm;
    (void)bufs;
    (void)n_bufs;

    return RAFT_BUSY;
}

static int fakeSnapshotPut(struct raft_io *io, unsigned trailing,
                           struct raft_io_snapshot_put *req,
                           const struct raft_snapshot *snapshot,
                           raft_io_snapshot_put_cb cb)
{
    (void)io;
    (void)trailing;
    (void)req;
    (void)snapshot;
    (void)cb;

    return RAFT_NOMEM;
}

/* Install a snapshot on a follower that has fallen behind. */
TEST(snapshot, configurationCopyFail, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    struct raft_install_snapshot snap = {.term = 2};
    typeof(fakeSnapshot) *snapshot;
    typeof(fakeSnapshotPut) *put;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    will_return(configurationCopy, RAFT_NOMEM);
    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    snapshot = CLUSTER_RAFT(0)->fsm->snapshot;
    CLUSTER_RAFT(0)->fsm->snapshot = fakeSnapshot;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_RAFT(0)->fsm->snapshot = snapshot;
    put = CLUSTER_RAFT(0)->io->snapshot_put;
    CLUSTER_RAFT(0)->io->snapshot_put = fakeSnapshotPut;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_RAFT(0)->io->snapshot_put = put;
    CLUSTER_MAKE_PROGRESS;

    /* Reconnect the follower and wait for it to catch up */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_SNAPSHOT(&f->cluster, 0, 2, &snap, 3000);

    return MUNIT_OK;
}

static struct raft_snapshot *createSnapshot(struct raft *r)
{
    struct raft_snapshot *snapshot;

    snapshot = raft_malloc(sizeof(*snapshot));
    munit_assert_ptr_not_null(snapshot);
    snapshot->n_bufs = 1;
    snapshot->bufs = raft_calloc(snapshot->n_bufs, sizeof(struct raft_buffer));
    munit_assert_ptr_not_null(snapshot->bufs);
    configurationCopy(&r->configuration, &snapshot->configuration);
    snapshot->configuration_index = r->configuration_index;
    snapshot->index = r->last_applied;

    return snapshot;
}

static int fakeRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    (void)fsm;
    (void)buf;

    return 0;
}

TEST(snapshot, restoreSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    int rv;
    struct raft_snapshot *snapshot;
    typeof(fakeRestore) *restore;

    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    CLUSTER_STEP_UNTIL_APPLIED(0, 1, 1000);

    restore = CLUSTER_RAFT(0)->fsm->restore;
    CLUSTER_RAFT(0)->fsm->restore = fakeRestore;
    snapshot = createSnapshot(CLUSTER_RAFT(0));
    munit_assert_ptr_not_null(snapshot);
    will_return(configurationCopy, RAFT_NOMEM);
    rv = snapshotRestore(CLUSTER_RAFT(0), snapshot);
    munit_assert_int(rv, ==, RAFT_NOMEM);
    snapshotDestroy(snapshot);

    snapshot = createSnapshot(CLUSTER_RAFT(0));
    munit_assert_ptr_not_null(snapshot);
    snapshotRestore(CLUSTER_RAFT(0), snapshot);
    CLUSTER_RAFT(0)->fsm->restore = restore;
    raft_free(snapshot);

    return MUNIT_OK;
}

/*send AE form disk to a follower that has fallen behind. */
TEST(snapshot, sendAeFromDisk0, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    return MUNIT_SKIP;
    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    ENABLE_CHANGE_AND_FREE_TRAILING;

    CLUSTER_DISCONNECT(0, 2);
    CLUSTER_DISCONNECT(2, 0);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 4, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 5, 5000);
    /* Reconnect the follower and wait for it to catch up */
    CLUSTER_RECONNECT(0, 2);
    CLUSTER_RECONNECT(2, 0);
    CLUSTER_STEP_UNTIL_APPLIED(2, 5, 5000);

    /* Check that the leader has sent a snapshot */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(2, 6, 2000);
    return MUNIT_OK;
}

/*send AE form disk to a follower that has fallen behind. */
TEST(snapshot, sendAeFromDisk, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    const struct raft *leader = CLUSTER_RAFT(0);
    return MUNIT_SKIP;
    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    ENABLE_CHANGE_AND_FREE_TRAILING;

    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    CLUSTER_STEP_UNTIL_APPLIED(2,5,2000);
    CLUSTER_DISCONNECT(0, 2);
    CLUSTER_DISCONNECT(2, 0);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 6, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 7, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 8, 5000);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(0, 9, 5000);
    /* Reconnect the follower and wait for it to catch up */
    munit_assert_uint(leader->snapshot.trailing, ==, 6);
    CLUSTER_RECONNECT(0, 2);
    CLUSTER_RECONNECT(2, 0);
    CLUSTER_STEP_UNTIL_APPLIED(2, 9, 5000);

    /* Check that the leader has sent a snapshot */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(2, 10, 2000);
    return MUNIT_OK;
}

/*send AE form disk to a follower that has fallen behind. */
TEST(snapshot, keepTrailingTimeout, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    const struct raft *leader = CLUSTER_RAFT(0);
    unsigned j;
    return MUNIT_SKIP;
    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    ENABLE_CHANGE_AND_FREE_TRAILING;

    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Apply a few of entries, to force a snapshot to be taken. */

    for(j = 0; j < 10; j++)
        CLUSTER_MAKE_PROGRESS;
    munit_assert_uint(leader->snapshot.trailing, ==, 12);
    /* Wait reset_trailing_timeout so leader detects offline node and reset trailing */
    CLUSTER_STEP_UNTIL_ELAPSED(33000);
    munit_assert_uint(leader->snapshot.trailing, ==, 3);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_APPLIED(2, 11, 5000);

    /* Check that the leader has sent a snapshot */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 1);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(2, 12, 2000);
    CLUSTER_STEP_UNTIL_APPLIED(2, 13, 2000);
    return MUNIT_OK;
}

/*send AE form disk to a follower that has fallen behind. */
TEST(snapshot, trailingMockIncrRefs, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    struct raft *leader = CLUSTER_RAFT(0);
    return MUNIT_SKIP;
    /* Set very low threshold and trailing entries number */
    SET_SNAPSHOT_THRESHOLD(3);
    SET_SNAPSHOT_TRAILING(1);

    ENABLE_CHANGE_AND_FREE_TRAILING;

    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Apply a few of entries, to force a snapshot to be taken. */
    CLUSTER_MAKE_PROGRESS;
    refsIncrTest(&leader->log, leader->log.entries[1].term, 2);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    refsDecrTest(&leader->log, leader->log.entries[1].term, 2);
    CLUSTER_MAKE_PROGRESS;
    //leader take snapshot at 3，因为index 2的日志还在被引用，所以只释放了index 1,3
    //leader take snapshot at 6，从index 2处开始释放，最终释放了index 2,4,5,6
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    /* Reconnect the follower and wait for it to catch up */
    munit_assert_uint(leader->snapshot.trailing, ==, 9);
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_APPLIED(2, 8, 5000);

    /* Check that the leader has sent a snapshot */
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    munit_assert_int(CLUSTER_N_RECV(2, RAFT_IO_INSTALL_SNAPSHOT), ==, 0);
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(2, 9, 2000);

    return MUNIT_OK;
}