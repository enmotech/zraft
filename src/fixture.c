#include "../include/raft/fixture.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "entry.h"
#include "log.h"
#include "queue.h"
#include "snapshot.h"
#include "tracing.h"
#include "replication.h"
#include "test/lib/fsm.h"
#include "../include/raft.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

/* Defaults */
#define HEARTBEAT_TIMEOUT 100
#define INSTALL_SNAPSHOT_TIMEOUT 30000
#define ELECTION_TIMEOUT 1000
#define NETWORK_LATENCY 15
#define DISK_LATENCY 10

/* To keep in sync with raft.h */
#define N_MESSAGE_TYPES 7

/* Maximum number of peer stub instances connected to a certain stub
 * instance. This should be enough for testing purposes. */
#define MAX_PEERS 8

/* Fields common across all request types. */
#define REQUEST                                                            \
    int type;                  /* Request code type. */                    \
    raft_time completion_time; /* When the request should be fulfilled. */ \
    queue queue                /* Link the I/O pending requests queue. */

/* Request type codes. */
enum { APPEND = 1, SEND, TRANSMIT, SNAPSHOT_PUT, SNAPSHOT_GET };

/* Abstract base type for an asynchronous request submitted to the stub I/o
 * implementation. */
struct ioRequest
{
    REQUEST;
};
static int g_fixture_errno = 0;

/* Pending request to append entries to the log. */
struct append
{
    REQUEST;
    struct raft_io_append *req;
    const struct raft_entry *entries;
    unsigned n;
    unsigned start; /* Request timestamp. */
};

/* Pending request to send a message. */
struct send
{
    REQUEST;
    struct raft_io_send *req;
    struct raft_message message;
};

/* Pending request to store a snapshot. */
struct snapshot_put
{
    REQUEST;
    unsigned trailing;
    struct raft_io_snapshot_put *req;
    const struct raft_snapshot *snapshot;
};

/* Pending request to load a snapshot. */
struct snapshot_get
{
    REQUEST;
    struct raft_io_snapshot_get *req;
};

/* Message that has been written to the network and is waiting to be delivered
 * (or discarded). */
struct transmit
{
    REQUEST;
    struct raft_message message; /* Message to deliver */
    int timer;                   /* Deliver after this n of msecs. */
};

/* Information about a peer server. */
struct peer
{
    struct io *io;  /* The peer's I/O backend. */
    bool connected; /* Whether a connection is established. */
    bool saturated; /* Whether the established connection is saturated. */
};

/* Stub I/O implementation implementing all operations in-memory. */
struct io
{
    struct raft_io *io;  /* I/O object we're implementing. */
    unsigned index;      /* Fixture server index. */
    raft_time *time;     /* Global cluster time. */
    raft_time next_tick; /* Time the next tick should occurs. */

    /* Term and vote */
    raft_term term;
    raft_id voted_for;

    /* Log */
    struct raft_snapshot *snapshot; /* Latest snapshot */
    struct raft_entry *entries;     /* Array or persisted entries */
    size_t n;                       /* Size of the persisted entries array */

    /* Parameters passed via raft_io->init and raft_io->start */
    raft_id id;
    unsigned tick_interval;
    raft_io_tick_cb tick_cb;
    raft_io_recv_cb recv_cb;

    /* Queue of pending asynchronous requests, whose callbacks still haven't
     * been fired. */
    queue requests;

    /* Peers connected to us. */
    struct peer peers[MAX_PEERS];
    unsigned n_peers;

    unsigned randomized_election_timeout; /* Value returned by io->random() */
    unsigned network_latency;             /* Milliseconds to deliver RPCs */
    unsigned disk_latency;                /* Milliseconds to perform disk I/O */

    struct
    {
        int countdown; /* Trigger the fault when this counter gets to zero. */
        int n;         /* Repeat the fault this many times. Default is -1. */
        unsigned mask; /* Trigger the falut only when bit mask is set. */
    } fault;

    /* If flag i is true, messages of type i will be silently dropped. */
    bool drop[N_MESSAGE_TYPES];

    /* Counters of events that happened so far. */
    unsigned n_send[N_MESSAGE_TYPES];
    unsigned n_recv[N_MESSAGE_TYPES];
    unsigned n_append;
};

/* Advance the fault counters and return @true if an error should occur. */
static bool ioFaultTick(struct io *io)
{
    /* If the countdown is negative, faults are disabled. */
    if (io->fault.countdown < 0) {
        return false;
    }

    /* If the countdown didn't reach zero, it's still not come the time to
     * trigger faults. */
    if (io->fault.countdown > 0) {
        io->fault.countdown--;
        return false;
    }

    assert(io->fault.countdown == 0);

    /* If n is negative we keep triggering the fault forever. */
    if (io->fault.n < 0) {
        return true;
    }

    /* If n is positive we need to trigger the fault at least this time. */
    if (io->fault.n > 0) {
        io->fault.n--;
        return true;
    }

    assert(io->fault.n == 0);

    /* We reached 'n', let's disable faults. */
    io->fault.countdown--;

    return false;
}

static int ioMethodInit(struct raft_io *raft_io,
                        raft_id id)
{
    struct io *io = raft_io->impl;
    io->id = id;
    return 0;
}

static int ioMethodStart(struct raft_io *raft_io,
                         unsigned msecs,
                         raft_io_tick_cb tick_cb,
                         raft_io_recv_cb recv_cb)
{
    struct io *io = raft_io->impl;
    if ((io->fault.mask & RAFT_IOFAULT_START) && ioFaultTick(io)) {
        return RAFT_IOERR;
    }
    io->tick_interval = msecs;
    io->tick_cb = tick_cb;
    io->recv_cb = recv_cb;
    io->next_tick = *io->time + io->tick_interval;
    return 0;
}

/* Flush an append entries request, appending its entries to the local in-memory
 * log. */
static void ioFlushAppend(struct io *s, struct append *append)
{
    struct raft_entry *entries;
    unsigned i;

    /* Allocate an array for the old entries plus the new ones. */
    entries = raft_realloc(s->entries, (s->n + append->n) * sizeof *s->entries);
    assert(entries != NULL);

    /* Copy new entries into the new array. */
    for (i = 0; i < append->n; i++) {
        const struct raft_entry *src = &append->entries[i];
        struct raft_entry *dst = &entries[s->n + i];
        int rv = entryCopy(src, dst);
        assert(rv == 0);
    }

    s->entries = entries;
    s->n += append->n;

    if (append->req->cb != NULL) {
        append->req->cb(append->req, 0);
    }
    raft_free(append);
}

/* Flush a snapshot put request, copying the snapshot data. */
static void ioFlushSnapshotPut(struct io *s, struct snapshot_put *r)
{
    int rv;

    if (s->snapshot == NULL) {
        s->snapshot = raft_malloc(sizeof *s->snapshot);
        assert(s->snapshot != NULL);
    } else {
        snapshotClose(s->snapshot);
    }

    rv = snapshotCopy(r->snapshot, s->snapshot);
    assert(rv == 0);

    if (r->trailing == 0) {
        rv = s->io->truncate(s->io, 1);
        assert(rv == 0);
    }

    if (r->req->cb != NULL) {
        r->req->cb(r->req, 0);
    }
    raft_free(r);
}

/* Flush a snapshot get request, returning to the client a copy of the local
 * snapshot (if any). */
static void ioFlushSnapshotGet(struct io *s, struct snapshot_get *r)
{
    struct raft_snapshot *snapshot;
    int rv;
    snapshot = raft_malloc(sizeof *snapshot);
    assert(snapshot != NULL);
    rv = snapshotCopy(s->snapshot, snapshot);
    assert(rv == 0);
    r->req->cb(r->req, snapshot, 0);
    raft_free(r);
}

/* Search for the peer with the given ID. */
static struct peer *ioGetPeer(struct io *io, raft_id id)
{
    unsigned i;
    for (i = 0; i < io->n_peers; i++) {
        struct peer *peer = &io->peers[i];
        if (peer->io->id == id) {
            return peer;
        }
    }
    return NULL;
}

/* Copy the dynamically allocated memory of an AppendEntries message. */
static void copyAppendEntries(const struct raft_append_entries *src,
                              struct raft_append_entries *dst)
{
    int rv;
    rv = entryBatchCopy(src->entries, &dst->entries, src->n_entries);
    assert(rv == 0);
    dst->trailing = src->trailing;
    dst->n_entries = src->n_entries;
}

/* Copy the dynamically allocated memory of an InstallSnapshot message. */
static void copyInstallSnapshot(const struct raft_install_snapshot *src,
                                struct raft_install_snapshot *dst)
{
    int rv;
    rv = configurationCopy(&src->conf, &dst->conf);
    assert(rv == 0);
    dst->data.base = raft_malloc(dst->data.len);
    assert(dst->data.base != NULL);
    memcpy(dst->data.base, src->data.base, src->data.len);
}
static void mockLoadEntries(struct io *io, struct raft_append_entries *dst, bool *flag)
{
    unsigned i;

    for (i = 0; i < dst->n_entries; i++){
        if(dst->entries[i].buf.len == 0 && dst->entries[i].buf.base == NULL && dst->entries[i].type != RAFT_BARRIER){
            flag[i] = true;
            raft_index cur_index = dst->prev_log_index + i + 1;
            entryCopy(&io->entries[cur_index - 1], &dst->entries[i]);
        } else {
            flag[i] = false;
        }
    }
}

/* Flush a raft_io_send request, copying the message content into a new struct
 * transmit object and invoking the user callback. */
static void ioFlushSend(struct io *io, struct send *send)
{
    struct peer *peer;
    struct transmit *transmit;
    struct raft_message *src;
    struct raft_message *dst;
    bool *flags = NULL;
    unsigned i;
    int status;

    /* If the peer doesn't exist or was disconnected, fail the request. */
    peer = ioGetPeer(io, send->message.server_id);
    if (peer == NULL || !peer->connected) {
        status = RAFT_NOCONNECTION;
        goto out;
    }

    transmit = raft_malloc(sizeof *transmit);
    assert(transmit != NULL);

    transmit->type = TRANSMIT;
    transmit->completion_time = *io->time + io->network_latency;

    src = &send->message;
    dst = &transmit->message;

    QUEUE_PUSH(&io->requests, &transmit->queue);

    if(src->type == RAFT_IO_APPEND_ENTRIES && src->append_entries.n_entries) {
        flags = raft_malloc(src->append_entries.n_entries * sizeof(bool));
        mockLoadEntries(io, &src->append_entries, flags);
    }

    *dst = *src;
    switch (dst->type) {
        case RAFT_IO_APPEND_ENTRIES:
            /* Make a copy of the entries being sent */
            copyAppendEntries(&src->append_entries, &dst->append_entries);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            copyInstallSnapshot(&src->install_snapshot, &dst->install_snapshot);
            break;
    }

    io->n_send[send->message.type]++;
    status = g_fixture_errno;
    if (src->type == RAFT_IO_APPEND_ENTRIES) {
        for (i = 0; i < src->append_entries.n_entries; i++)
            if (flags[i] == true){
                raft_entry_free(src->append_entries.entries[i].buf.base);
            }
    }
    raft_free(flags);
out:
    if (send->req->cb != NULL) {
        send->req->cb(send->req, status);
    }

    raft_free(send);
}

/* Release the memory used by the given message transmit object. */
static void ioDestroyTransmit(struct transmit *transmit)
{
    struct raft_message *message;
    message = &transmit->message;
    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            if (message->append_entries.entries != NULL) {
                raft_free(message->append_entries.entries[0].batch);
                raft_free(message->append_entries.entries);
            }
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            raft_configuration_close(&message->install_snapshot.conf);
            raft_free(message->install_snapshot.data.base);
            break;
    }
    raft_free(transmit);
}

/* Flush all requests in the queue. */
static void ioFlushAll(struct io *io)
{
    while (!QUEUE_IS_EMPTY(&io->requests)) {
        queue *head;
        struct ioRequest *r;

        head = QUEUE_HEAD(&io->requests);
        QUEUE_REMOVE(head);

        r = QUEUE_DATA(head, struct ioRequest, queue);
        switch (r->type) {
            case APPEND:
                ioFlushAppend(io, (struct append *)r);
                break;
            case SEND:
                ioFlushSend(io, (struct send *)r);
                break;
            case TRANSMIT:
                ioDestroyTransmit((struct transmit *)r);
                break;
            case SNAPSHOT_PUT:
                ioFlushSnapshotPut(io, (struct snapshot_put *)r);
                break;
            case SNAPSHOT_GET:
                ioFlushSnapshotGet(io, (struct snapshot_get *)r);
                break;
            default:
                assert(0);
        }
    }
}

static void ioMethodClose(struct raft_io *raft_io, bool clean, raft_io_close_cb cb)
{
    (void)clean;
    if (cb != NULL) {
        cb(raft_io);
    }
}

static int ioMethodLoad(struct raft_io *io,
                        raft_term *term,
                        raft_id *voted_for,
                        struct raft_snapshot **snapshot,
                        raft_index *start_index,
                        struct raft_entry **entries,
                        size_t *n_entries)
{
    struct io *s;
    int rv;

    s = io->impl;

    if ((s->fault.mask & RAFT_IOFAULT_LOAD) && ioFaultTick(s)) {
        return RAFT_IOERR;
    }

    *term = s->term;
    *voted_for = s->voted_for;
    *start_index = 1;

    *n_entries = s->n;

    /* Make a copy of the persisted entries, storing their data into a single
     * batch. */
    rv = entryBatchCopy(s->entries, entries, s->n);
    assert(rv == 0);

    if (s->snapshot != NULL) {
        *snapshot = raft_malloc(sizeof **snapshot);
        assert(*snapshot != NULL);
        rv = snapshotCopy(s->snapshot, *snapshot);
        assert(rv == 0);
        *start_index = (*snapshot)->index + 1;
    } else {
        *snapshot = NULL;
    }

    return 0;
}

static int ioMethodBootstrap(struct raft_io *raft_io,
                             const struct raft_configuration *conf)
{
    struct io *io = raft_io->impl;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    if ((io->fault.mask & RAFT_IOFAULT_BOOTSTRAP) && ioFaultTick(io)) {
        return RAFT_IOERR;
    }

    if (io->term != 0) {
        return RAFT_CANTBOOTSTRAP;
    }

    assert(io->voted_for == 0);
    assert(io->snapshot == NULL);
    assert(io->entries == NULL);
    assert(io->n == 0);

    /* Encode the given configuration. */
    rv = configurationEncode(conf, &buf);
    if (rv != 0) {
        return rv;
    }

    entries = raft_calloc(1, sizeof *io->entries);
    if (entries == NULL) {
        return RAFT_NOMEM;
    }

    entries[0].term = 1;
    entries[0].type = RAFT_CHANGE;
    entries[0].buf = buf;

    io->term = 1;
    io->voted_for = 0;
    io->snapshot = NULL;
    io->entries = entries;
    io->n = 1;

    return 0;
}

static int ioMethodRecover(struct raft_io *io,
                           const struct raft_configuration *conf)
{
    /* TODO: implement this API */
    (void)io;
    (void)conf;
    return RAFT_IOERR;
}

static int ioMethodSetMeta(struct raft_io *raft_io,
                           struct raft_io_set_meta *req,
                           raft_term term,
                           raft_id vote,
                           raft_io_set_meta_cb cb)
{
    struct io *io = raft_io->impl;

    if ((io->fault.mask & RAFT_IOFAULT_SETMETA) && ioFaultTick(io)) {
        cb(req, RAFT_IOERR);
        return 0;
    }

    io->term = term;
    io->voted_for = vote;

    cb(req, 0);
    return 0;
}

static int ioMethodAppend(struct raft_io *raft_io,
                          struct raft_io_append *req,
                          const struct raft_entry entries[],
                          unsigned n,
                          raft_io_append_cb cb)
{
    struct io *io = raft_io->impl;
    struct append *r;

    if ((io->fault.mask & RAFT_IOFAULT_APPEND) && ioFaultTick(io)) {
        return RAFT_IOERR;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = APPEND;
    r->completion_time = *io->time + io->disk_latency;
    r->req = req;
    r->entries = entries;
    r->n = n;

    req->cb = cb;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static int ioMethodTruncate(struct raft_io *raft_io, raft_index index)
{
    struct io *io = raft_io->impl;
    size_t n;

    if ((io->fault.mask & RAFT_IOFAULT_TRUNCATE) && ioFaultTick(io)) {
        return RAFT_IOERR;
    }

    n = (size_t)(index - 1); /* Number of entries left after truncation */

    if (n > 0) {
        struct raft_entry *entries;

        /* Create a new array of entries holding the non-truncated entries */
        entries = raft_malloc(n * sizeof *entries);
        if (entries == NULL) {
            return RAFT_NOMEM;
        }
        memcpy(entries, io->entries, n * sizeof *io->entries);

        /* Release any truncated entry */
        if (io->entries != NULL) {
            size_t i;
            for (i = n; i < io->n; i++) {
		    if (io->entries[i].buf.base != NULL)
			    raft_free(io->entries[i].buf.base);
            }
            raft_free(io->entries);
        }
        io->entries = entries;
    } else {
        /* Release everything we have */
	if (io->entries != NULL) {
	    size_t i;
	    for (i = 0; i < io->n; i++) {
		    if (io->entries[i].buf.base != NULL)
			    raft_free(io->entries[i].buf.base);
	    }
	    raft_free(io->entries);
	    io->entries = NULL;
        }
    }

    io->n = n;

    return 0;
}

static int ioMethodSnapshotPut(struct raft_io *raft_io,
                               unsigned trailing,
                               struct raft_io_snapshot_put *req,
                               const struct raft_snapshot *snapshot,
                               raft_io_snapshot_put_cb cb)
{
    struct io *io = raft_io->impl;
    struct snapshot_put *r;

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SNAPSHOT_PUT;
    r->req = req;
    r->req->cb = cb;
    r->snapshot = snapshot;
    r->completion_time = *io->time + io->disk_latency;
    r->trailing = trailing;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static int ioMethodSnapshotGet(struct raft_io *raft_io,
                               struct raft_io_snapshot_get *req,
                               raft_io_snapshot_get_cb cb)
{
    struct io *io = raft_io->impl;
    struct snapshot_get *r;

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SNAPSHOT_GET;
    r->req = req;
    r->req->cb = cb;
    r->completion_time = *io->time + io->disk_latency;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static raft_time ioMethodTime(struct raft_io *raft_io)
{
    struct io *io = raft_io->impl;
    return *io->time;
}

static int ioMethodRandom(struct raft_io *raft_io, int min, int max)
{
    struct io *io;
    (void)min;
    (void)max;
    io = raft_io->impl;
    return (int)io->randomized_election_timeout;
}

/* Queue up a request which will be processed later, when io_stub_flush()
 * is invoked. */
static int ioMethodSend(struct raft_io *raft_io,
                        struct raft_io_send *req,
                        const struct raft_message *message,
                        raft_io_send_cb cb)
{
    struct io *io = raft_io->impl;
    struct send *r;
    if ((io->fault.mask & RAFT_IOFAULT_SEND) && ioFaultTick(io)) {
        return RAFT_IOERR;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SEND;
    r->req = req;
    r->message = *message;
    r->req->cb = cb;

    /* TODO: simulate the presence of an OS send buffer, whose available size
     * might delay the completion of send requests */
    r->completion_time = *io->time;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static void ioReceive(struct io *io, struct raft_message *message)
{
    io->recv_cb(io->io, message);
    io->n_recv[message->type]++;
}

static void ioDeliverTransmit(struct io *io, struct transmit *transmit)
{
    struct raft_message *message = &transmit->message;
    struct peer *peer; /* Destination peer */

    /* If this message type is in the drop list, let's discard it */
    if (io->drop[message->type - 1]) {
        ioDestroyTransmit(transmit);
        return;
    }

    peer = ioGetPeer(io, message->server_id);

    /* We don't have any peer with this ID or it's disconnected or if the
     * connection is saturated, let's drop the message */
    if (peer == NULL || !peer->connected || peer->saturated) {
        ioDestroyTransmit(transmit);
        return;
    }

    /* Update the message object with our details. */
    message->server_id = io->id;

    ioReceive(peer->io, message);
    raft_free(transmit);
}

/* Connect @raft_io to @other, enabling delivery of messages sent from @io to
 * @other.
 */
static void ioConnect(struct raft_io *raft_io, struct raft_io *other)
{
    struct io *io = raft_io->impl;
    struct io *io_other = other->impl;
    assert(io->n_peers < MAX_PEERS);
    io->peers[io->n_peers].io = io_other;
    io->peers[io->n_peers].connected = true;
    io->peers[io->n_peers].saturated = false;
    io->n_peers++;
}

/* Return whether the connection with the given peer is saturated. */
static bool ioSaturated(struct raft_io *raft_io, struct raft_io *other)
{
    struct io *io = raft_io->impl;
    struct io *io_other = other->impl;
    struct peer *peer;
    peer = ioGetPeer(io, io_other->id);
    return peer != NULL && peer->saturated;
}

/* Disconnect @raft_io and @other, causing calls to @io->send() to fail
 * asynchronously when sending messages to @other. */
static void ioDisconnect(struct raft_io *raft_io, struct raft_io *other)
{
    struct io *io = raft_io->impl;
    struct io *io_other = other->impl;
    struct peer *peer;
    peer = ioGetPeer(io, io_other->id);
    assert(peer != NULL);
    peer->connected = false;
}

/* Reconnect @raft_io and @other. */
static void ioReconnect(struct raft_io *raft_io, struct raft_io *other)
{
    struct io *io = raft_io->impl;
    struct io *io_other = other->impl;
    struct peer *peer;
    peer = ioGetPeer(io, io_other->id);
    assert(peer != NULL);
    peer->connected = true;
}

/* Saturate the connection from @io to @other, causing messages sent from @io to
 * @other to be dropped. */
static void ioSaturate(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = ioGetPeer(s, s_other->id);
    peer->saturated = true;
}

/* Desaturate the connection from @raft_io to @other, re-enabling delivery of
 * messages sent from @raft_io to @other. */
static void ioDesaturate(struct raft_io *raft_io, struct raft_io *other)
{
    struct io *io = raft_io->impl;
    struct io *io_other = other->impl;
    struct peer *peer;
    peer = ioGetPeer(io, io_other->id);
    assert(peer != NULL && peer->connected);
    peer->saturated = false;
}

/* Enable or disable silently dropping all outgoing messages of type @type. */
void ioDrop(struct io *io, int type, bool flag)
{
    io->drop[type - 1] = flag;
}

static int ioInit(struct raft_io *raft_io, unsigned index, raft_time *time)
{
    struct io *io;
    io = raft_malloc(sizeof *io);
    assert(io != NULL);
    io->io = raft_io;
    io->index = index;
    io->time = time;
    io->term = 0;
    io->voted_for = 0;
    io->snapshot = NULL;
    io->entries = NULL;
    io->n = 0;
    QUEUE_INIT(&io->requests);
    io->n_peers = 0;
    io->randomized_election_timeout = ELECTION_TIMEOUT + index * 100;
    io->network_latency = NETWORK_LATENCY;
    io->disk_latency = DISK_LATENCY;
    io->fault.countdown = -1;
    io->fault.n = -1;
    io->fault.mask = RAFT_IOFAULT_ALL;
    memset(io->drop, 0, sizeof io->drop);
    memset(io->n_send, 0, sizeof io->n_send);
    memset(io->n_recv, 0, sizeof io->n_recv);
    io->n_append = 0;

    raft_io->impl = io;
    raft_io->init = ioMethodInit;
    raft_io->close = ioMethodClose;
    raft_io->start = ioMethodStart;
    raft_io->load = ioMethodLoad;
    raft_io->bootstrap = ioMethodBootstrap;
    raft_io->recover = ioMethodRecover;
    //raft_io->set_term = ioMethodSetTerm;
    //raft_io->set_vote = ioMethodSetVote;
    raft_io->set_meta = ioMethodSetMeta;
    raft_io->append = ioMethodAppend;
    raft_io->truncate = ioMethodTruncate;
    raft_io->send = ioMethodSend;
    raft_io->snapshot_put = ioMethodSnapshotPut;
    raft_io->snapshot_get = ioMethodSnapshotGet;
    raft_io->time = ioMethodTime;
    raft_io->time_us = ioMethodTime;
    raft_io->random = ioMethodRandom;

    return 0;
}

/* Release all memory held by the given stub I/O implementation. */
void ioClose(struct raft_io *raft_io)
{
    struct io *io = raft_io->impl;
    size_t i;
    for (i = 0; i < io->n; i++) {
        struct raft_entry *entry = &io->entries[i];
	if (entry->buf.base != NULL)
		raft_free(entry->buf.base);
    }
    if (io->entries != NULL) {
        raft_free(io->entries);
    }
    if (io->snapshot != NULL) {
        snapshotClose(io->snapshot);
        raft_free(io->snapshot);
    }
    raft_free(io);
}

/* Custom emit tracer function which include the server ID. */
static void emit(struct raft_tracer *t,
                 const char *file,
                 int line,
                 const char *message)
{
    struct raft_fixture_server *s = t->impl;

    fprintf(stderr, "[%6llu] %30s:%*d - server %lld : %s\n", s->f->time,
        file, 3, line, s->id, message);
}

static int serverInit(struct raft_fixture *f, unsigned i, struct raft_fsm *fsm)
{
    int rv;
    struct raft_fixture_server *s = &f->servers[i];
    s->alive = true;
    s->id = i + 1;
    rv = ioInit(&s->io, i, &f->time);
    if (rv != 0) {
        return rv;
    }
    rv = raft_init(&s->raft, &s->io, fsm, s->id);
    if (rv != 0) {
        return rv;
    }
    raft_set_election_timeout(&s->raft, ELECTION_TIMEOUT);
    raft_set_heartbeat_timeout(&s->raft, HEARTBEAT_TIMEOUT);
    raft_set_install_snapshot_timeout(&s->raft, INSTALL_SNAPSHOT_TIMEOUT);
    s->f = f;
    s->tracer.impl = (void *)s;
    s->tracer.emit = emit;
    s->raft.tracer = &s->tracer;
    return 0;
}

static void serverClose(struct raft_fixture_server *s)
{
    raft_close(&s->raft, false, NULL);
    ioClose(&s->io);
}

/* Connect the server with the given index to all others */
static void serverConnectToAll(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        if (i == j) {
            continue;
        }
        ioConnect(io1, io2);
    }
}

int raft_fixture_init(struct raft_fixture *f, unsigned n, struct raft_fsm *fsms)
{
    unsigned i;
    int rc;
    assert(n >= 1);

    f->time = 0;
    f->n = n;

    /* Initialize all servers */
    for (i = 0; i < n; i++) {
        rc = serverInit(f, i, &fsms[i]);
        if (rc != 0) {
            return rc;
        }
    }

    /* Connect all servers to each another */
    for (i = 0; i < f->n; i++) {
        serverConnectToAll(f, i);
    }

    logInit(&f->log);
    f->commit_index = 0;
    f->hook = NULL;

    return 0;
}

void raft_fixture_close(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct io *io = f->servers[i].io.impl;
        ioFlushAll(io);
    }
    for (i = 0; i < f->n; i++) {
        serverClose(&f->servers[i]);
    }
    logClose(&f->log);
}

int raft_fixture_configuration(struct raft_fixture *f,
                               unsigned n_voting,
                               struct raft_configuration *configuration)
{
    unsigned i;
    assert(f->n > 0);
    assert(n_voting > 0);
    assert(n_voting <= f->n);
    raft_configuration_init(configuration);
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s;
        int role = i < n_voting ? RAFT_VOTER : RAFT_STANDBY;
        int rv;
        s = &f->servers[i];
        rv = raft_configuration_add(configuration, s->id, role);
        if (rv != 0) {
            return rv;
        }
    }
    return 0;
}

int raft_fixture_bootstrap(struct raft_fixture *f,
                           struct raft_configuration *configuration)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft *raft = raft_fixture_get(f, i);
        int rv;
        rv = raft_bootstrap(raft, configuration);
        if (rv != 0) {
            return rv;
        }
    }
    return 0;
}

int raft_fixture_start(struct raft_fixture *f)
{
    unsigned i;
    int rv;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        rv = raft_start(&s->raft);
        if (rv != 0) {
            return rv;
        }
    }
    return 0;
}

unsigned raft_fixture_n(struct raft_fixture *f)
{
    return f->n;
}

raft_time raft_fixture_time(struct raft_fixture *f)
{
    return f->time;
}

struct raft *raft_fixture_get(struct raft_fixture *f, unsigned i)
{
    assert(i < f->n);
    return &f->servers[i].raft;
}

bool raft_fixture_alive(struct raft_fixture *f, unsigned i)
{
    assert(i < f->n);
    return f->servers[i].alive;
}

unsigned raft_fixture_leader_index(struct raft_fixture *f)
{
    if (f->leader_id != 0) {
        return (unsigned)(f->leader_id - 1);
    }
    return f->n;
}

raft_id raft_fixture_voted_for(struct raft_fixture *f, unsigned i)
{
    struct io *io = f->servers[i].io.impl;
    return io->voted_for;
}

/* Update the leader and check for election safety.
 *
 * From figure 3.2:
 *
 *   Election Safety -> At most one leader can be elected in a given
 *   term.
 *
 * Return true if the current leader turns out to be different from the one at
 * the time this function was called.
 */
static bool updateLeaderAndCheckElectionSafety(struct raft_fixture *f)
{
    raft_id leader_id = 0;
    unsigned leader_i = 0;
    raft_term leader_term = 0;
    unsigned i;
    bool changed;

    for (i = 0; i < f->n; i++) {
        struct raft *raft = raft_fixture_get(f, i);
        unsigned j;

        /* If the server is not alive or is not the leader, skip to the next
         * server. */
        if (!raft_fixture_alive(f, i) || raft_state(raft) != RAFT_LEADER) {
            continue;
        }

        /* Check that no other server is leader for this term. */
        for (j = 0; j < f->n; j++) {
            struct raft *other = raft_fixture_get(f, j);

            if (other->id == raft->id || other->state != RAFT_LEADER) {
                continue;
            }

            if (other->current_term == raft->current_term) {
                fprintf(stderr,
                        "server %llu and %llu are both leaders in term %llu",
                        raft->id, other->id, raft->current_term);
                abort();
            }
        }

        if (raft->current_term > leader_term) {
            leader_id = raft->id;
            leader_i = i;
            leader_term = raft->current_term;
        }
    }

    /* Check that the leader is stable, in the sense that it has been
     * acknowledged by all alive servers connected to it, and those servers
     * together with the leader form a majority. */
    if (leader_id != 0) {
        unsigned n_acks = 0;
        bool acked = true;
        unsigned n_quorum = 0;

        struct raft *leader = raft_fixture_get(f, (unsigned int)(leader_id-1));
        for (i = 0; i < f->n; i++) {
            struct raft *raft = raft_fixture_get(f, i);
            const struct raft_server *server =
                configurationGet(&leader->configuration, raft->id);

            /* If the server is not in the configuration or is idle, then don't
             * count it. */
            if (server == NULL || server->role == RAFT_SPARE) {
                continue;
            }

            n_quorum++;

            /* If this server is itself the leader, or it's not alive or it's
             * not connected to the leader, then don't count it in for
             * stability. */
            if (i == leader_i || !raft_fixture_alive(f, i) ||
                raft_fixture_saturated(f, leader_i, i)) {
                continue;
            }

            if (raft->current_term != leader_term) {
                acked = false;
                break;
            }

            if (raft->state != RAFT_FOLLOWER) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader.id == 0) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader.id != leader_id) {
                acked = false;
                break;
            }

            n_acks++;
        }
        if (!acked || n_acks < (n_quorum / 2)) {
            leader_id = 0;
        }
    }

    changed = leader_id != f->leader_id;
    f->leader_id = leader_id;

    return changed;
}

/* Check for leader append-only.
 *
 * From figure 3.2:
 *
 *   Leader Append-Only -> A leader never overwrites or deletes entries in its
 *   own log; it only appends new entries.
 */
static void checkLeaderAppendOnly(struct raft_fixture *f)
{
    struct raft *raft;
    raft_index index;
    raft_index last = logLastIndex(&f->log);

    /* If the cached log is empty it means there was no leader before. */
    if (last == 0) {
        return;
    }

    /* If there's no new leader, just return. */
    if (f->leader_id == 0) {
        return;
    }

    raft = raft_fixture_get(f, (unsigned)f->leader_id - 1);
    last = logLastIndex(&f->log);

    for (index = 1; index <= last; index++) {
        const struct raft_entry *entry1;
        const struct raft_entry *entry2;
        size_t i;

        entry1 = logGet(&f->log, index);
        entry2 = logGet(&raft->log, index);

        assert(entry1 != NULL);

        /* Check if the entry was snapshotted. */
        if (entry2 == NULL) {
            assert(raft->log.snapshot.last_index >= index);
            continue;
        }

        /* Entry was not overwritten. */
        assert(entry1->type == entry2->type);
        assert(entry1->term == entry2->term);
        for (i = 0; i < entry1->buf.len; i++) {
            assert(((uint8_t *)entry1->buf.base)[i] ==
                   ((uint8_t *)entry2->buf.base)[i]);
        }
    }
}

/* Make a copy of the the current leader log, in order to perform the Leader
 * Append-Only check at the next iteration. */
static void copyLeaderLog(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, (unsigned)f->leader_id - 1);
    struct raft_entry *entries;
    unsigned n;
    unsigned total;
    size_t i;
    int rv;
    logClose(&f->log);
    logInit(&f->log);
    total = (unsigned)logNumEntriesFromIndex(&raft->log, 1);
    if (total == 0) {
        return;
    }
    entries = raft_calloc(total, sizeof(*entries));
    assert(entries);
    rv = logAcquire(&raft->log, 1, &entries, &n, total);
    assert(rv == 0);
    assert(n == total);
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        struct raft_buffer buf;
        buf.len = entry->buf.len;
	    if (buf.len > 0) {
		    buf.base = raft_entry_malloc(buf.len);
		    assert(buf.base != NULL);
		    memcpy(buf.base, entry->buf.base, buf.len);
	    } else {
		    buf.base = NULL;
        }
        rv = logAppend(&f->log, entry->term, entry->type, &buf, NULL);
        assert(rv == 0);
    }
    logRelease(&raft->log, 1, entries, n);
    raft_free(entries);
}

/* Update the commit index to match the one from the current leader. */
static void updateCommitIndex(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, (unsigned)f->leader_id - 1);
    if (raft->commit_index > f->commit_index) {
        f->commit_index = raft->commit_index;
    }
}

/* Return the lowest tick time across all servers, along with the associated
 * server index */
static void getLowestTickTime(struct raft_fixture *f, raft_time *t, unsigned *i)
{
    unsigned j;
    *t = (raft_time)-1 /* Maximum value */;
    for (j = 0; j < f->n; j++) {
        struct io *io = f->servers[j].io.impl;
        if (io->next_tick < *t) {
            *t = io->next_tick;
            *i = j;
        }
    }
}

/* Return the completion time of the request with the lowest completion time
 * across all servers, along with the associated server index. */
static void getLowestRequestCompletionTime(struct raft_fixture *f,
                                           raft_time *t,
                                           unsigned *i)
{
    unsigned j;
    *t = (raft_time)-1 /* Maximum value */;
    for (j = 0; j < f->n; j++) {
        struct io *io = f->servers[j].io.impl;
        queue *head;
        QUEUE_FOREACH(head, &io->requests)
        {
            struct ioRequest *r = QUEUE_DATA(head, struct ioRequest, queue);
            if (r->completion_time < *t) {
                *t = r->completion_time;
                *i = j;
            }
        }
    }
}

/* Fire the tick callback of the i'th server. */
static void fireTick(struct raft_fixture *f, unsigned i)
{
    struct io *io = f->servers[i].io.impl;
    f->time = io->next_tick;
    f->event.server_index = i;
    f->event.type = RAFT_FIXTURE_TICK;
    io->next_tick += io->tick_interval;
    io->tick_cb(io->io);
}

/* Complete the first request with completion time @t on the @i'th server. */
static void completeRequest(struct raft_fixture *f, unsigned i, raft_time t)
{
    struct io *io = f->servers[i].io.impl;
    queue *head;
    struct ioRequest *r = NULL;
    bool found = false;
    f->time = t;
    f->event.server_index = i;
    QUEUE_FOREACH(head, &io->requests)
    {
        r = QUEUE_DATA(head, struct ioRequest, queue);
        if (r->completion_time == t) {
            found = true;
            break;
        }
    }
    assert(found);
    QUEUE_REMOVE(head);
    switch (r->type) {
        case APPEND:
            ioFlushAppend(io, (struct append *)r);
            f->event.type = RAFT_FIXTURE_DISK;
            break;
        case SEND:
            ioFlushSend(io, (struct send *)r);
            f->event.type = RAFT_FIXTURE_NETWORK;
            break;
        case TRANSMIT:
            ioDeliverTransmit(io, (struct transmit *)r);
            f->event.type = RAFT_FIXTURE_NETWORK;
            break;
        case SNAPSHOT_PUT:
            ioFlushSnapshotPut(io, (struct snapshot_put *)r);
            f->event.type = RAFT_FIXTURE_DISK;
            break;
        case SNAPSHOT_GET:
            ioFlushSnapshotGet(io, (struct snapshot_get *)r);
            f->event.type = RAFT_FIXTURE_DISK;
            break;
        default:
            assert(0);
    }
}

struct raft_fixture_event *raft_fixture_step(struct raft_fixture *f)
{
    raft_time tick_time;
    raft_time completion_time;
    unsigned i = f->n;
    unsigned j = f->n;

    getLowestTickTime(f, &tick_time, &i);
    getLowestRequestCompletionTime(f, &completion_time, &j);

    assert(i < f->n || j < f->n);

    if (tick_time < completion_time ||
        (tick_time == completion_time && i <= j)) {
        fireTick(f, i);
    } else {
        completeRequest(f, j, completion_time);
    }

    /* If the leader has not changed check the Leader Append-Only
     * guarantee. */
    if (!updateLeaderAndCheckElectionSafety(f)) {
        checkLeaderAppendOnly(f);
    }

    /* If we have a leader, update leader-related state . */
    if (f->leader_id != 0) {
        copyLeaderLog(f);
        updateCommitIndex(f);
    }

    if (f->hook != NULL) {
        f->hook(f, &f->event);
    }

    return &f->event;
}

struct raft_fixture_event *raft_fixture_step_n(struct raft_fixture *f,
                                               unsigned n)
{
    unsigned i;
    assert(n > 0);
    for (i = 0; i < n - 1; i++) {
        raft_fixture_step(f);
    }
    return raft_fixture_step(f);
}

bool raft_fixture_step_until(struct raft_fixture *f,
                             bool (*stop)(struct raft_fixture *f, void *arg),
                             void *arg,
                             unsigned max_msecs)
{
    raft_time start = f->time;
    while (!stop(f, arg) && (f->time - start) < max_msecs) {
        raft_fixture_step(f);
    }
    return f->time - start < max_msecs;
}

/* A step function which return always false, forcing raft_fixture_step_n to
 * advance time at each iteration. */
static bool spin(struct raft_fixture *f, void *arg)
{
    (void)f;
    (void)arg;
    return false;
}

void raft_fixture_step_until_elapsed(struct raft_fixture *f, unsigned msecs)
{
    raft_fixture_step_until(f, spin, NULL, msecs);
}

static bool hasLeader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id != 0;
}

bool raft_fixture_step_until_has_leader(struct raft_fixture *f,
                                        unsigned max_msecs)
{
    return raft_fixture_step_until(f, hasLeader, NULL, max_msecs);
}

static bool hasNoLeader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id == 0;
}

bool raft_fixture_step_until_has_no_leader(struct raft_fixture *f,
                                           unsigned max_msecs)
{
    return raft_fixture_step_until(f, hasNoLeader, NULL, max_msecs);
}

/* Enable/disable dropping outgoing messages of a certain type from all servers
 * except one. */
static void dropAllExcept(struct raft_fixture *f,
                          int type,
                          bool flag,
                          unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_fixture_server *s = &f->servers[j];
        if (j == i) {
            continue;
        }
        ioDrop(s->io.impl, type, flag);
    }
}

/* Set the randomized election timeout of the given server to the minimum value
 * compatible with its current state and timers. */
static void minimizeRandomizedElectionTimeout(struct raft_fixture *f,
                                              unsigned i)
{
    struct raft *raft = &f->servers[i].raft;
    raft_time now = raft->io->time(raft->io);
    unsigned timeout = raft->election_timeout;
    assert(raft->state == RAFT_FOLLOWER);

    /* If the minimum election timeout value would make the timer expire in the
     * past, cap it. */
    if (now - raft->election_timer_start > timeout) {
        timeout = (unsigned)(now - raft->election_timer_start);
    }

    raft->follower_state.randomized_election_timeout = timeout;
}

/* Set the randomized election timeout to the maximum value on all servers
 * except the given one. */
static void maximizeAllRandomizedElectionTimeoutsExcept(struct raft_fixture *f,
                                                        unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft *raft = &f->servers[j].raft;
        unsigned timeout = raft->election_timeout * 2;
        if (j == i) {
            continue;
        }
        assert(raft->state == RAFT_FOLLOWER);
        raft->follower_state.randomized_election_timeout = timeout;
    }
}
void raft_fixture_set_election_timeout_min(struct raft_fixture *f,
										   unsigned i)
{
	maximizeAllRandomizedElectionTimeoutsExcept(f, i);
}

void raft_fixture_hook(struct raft_fixture *f, raft_fixture_event_cb hook)
{
    f->hook = hook;
}

void raft_fixture_elect(struct raft_fixture *f, unsigned i)
{
    struct raft *raft = raft_fixture_get(f, i);
    unsigned j;

    /* Make sure there's currently no leader. */
    assert(f->leader_id == 0);

    /* Make sure that the given server is voting. */
    assert(configurationGet(&raft->configuration, raft->id)->role ==
           RAFT_VOTER);

    /* Make sure all servers are currently followers. */
    for (j = 0; j < f->n; j++) {
        assert(raft_state(&f->servers[j].raft) == RAFT_FOLLOWER);
    }

    /* Pretend that the last randomized election timeout was set at the maximum
     * value on all server expect the one to be elected, which is instead set to
     * the minimum possible value compatible with its current state. */
    minimizeRandomizedElectionTimeout(f, i);
    maximizeAllRandomizedElectionTimeoutsExcept(f, i);

    raft_fixture_step_until_has_leader(f, ELECTION_TIMEOUT * 20);
    assert(f->leader_id == raft->id);
}

void raft_fixture_depose(struct raft_fixture *f)
{
    unsigned leader_i;

    /* Make sure there's a leader. */
    assert(f->leader_id != 0);
    leader_i = (unsigned)f->leader_id - 1;
    assert(raft_state(&f->servers[leader_i].raft) == RAFT_LEADER);

    /* Set a very large election timeout on all followers, to prevent them from
     * starting an election. */
    maximizeAllRandomizedElectionTimeoutsExcept(f, leader_i);

    /* Prevent all servers from sending append entries results, so the leader
     * will eventually step down. */
    dropAllExcept(f, RAFT_IO_APPEND_ENTRIES_RESULT, true, leader_i);

    raft_fixture_step_until_has_no_leader(f, ELECTION_TIMEOUT * 3);
    assert(f->leader_id == 0);

    dropAllExcept(f, RAFT_IO_APPEND_ENTRIES_RESULT, false, leader_i);
}

struct step_apply
{
    unsigned i;
    raft_index index;
};

static bool hasAppliedIndex(struct raft_fixture *f, void *arg)
{
    struct step_apply *apply = (struct step_apply *)arg;
    struct raft *raft;
    unsigned n = 0;
    unsigned i;

    if (apply->i < f->n) {
        raft = raft_fixture_get(f, apply->i);
        return raft_last_applied(raft) >= apply->index;
    }

    for (i = 0; i < f->n; i++) {
        raft = raft_fixture_get(f, i);
        if (raft_last_applied(raft) >= apply->index) {
            n++;
        }
    }
    return n == f->n;
}

bool raft_fixture_step_until_applied(struct raft_fixture *f,
                                     unsigned i,
                                     raft_index index,
                                     unsigned max_msecs)
{
    struct step_apply apply = {i, index};
    return raft_fixture_step_until(f, hasAppliedIndex, &apply, max_msecs);
}

static bool hasAppendedIndex(struct raft_fixture *f, void *arg)
{
    struct step_apply *apply = (struct step_apply *)arg;
    struct raft *raft;
    unsigned n = 0;
    unsigned i;

    if (apply->i < f->n) {
        raft = raft_fixture_get(f, apply->i);
        return raft->last_stored >= apply->index;
    }

    for (i = 0; i < f->n; i++) {
        raft = raft_fixture_get(f, i);
        if (raft->last_stored >= apply->index) {
            n++;
        }
    }
    return n == f->n;
}

static bool hasConfirmedAppendIndex(struct raft_fixture *f, void *arg)
{
	struct step_apply *apply = (struct step_apply *)arg;
	struct raft *raft;
	unsigned leader_index;
	unsigned n = 0;
	unsigned i;

	if (f->leader_id == 0) {
		/* find the leader */
		for (i = 0; i < f->n; ++i) {
			raft = raft_fixture_get(f, i);
			if (raft_state(raft) == RAFT_LEADER)
				break;
			else
				assert(raft_state(raft) == RAFT_FOLLOWER);
		}
		assert(i < f->n);
		leader_index = i;
		/* check if this is the only leader */
		for (i = leader_index + 1; i < f->n; ++i) {
			raft = raft_fixture_get(f, i);
			assert(raft_state(raft) == RAFT_FOLLOWER);
		}
	} else
		leader_index = (unsigned)(f->leader_id) - 1;

	raft = raft_fixture_get(f, leader_index);
	assert(raft->state == RAFT_LEADER);
	i = apply->i;
	if (i < f->n)
		return raft->leader_state.progress[i].match_index >= apply->index;

	for (i = 0; i < f->n; i++) {
		if (raft->leader_state.progress[i].match_index >= apply->index) {
			n++;
		}
	}
	return n == f->n;
}

bool raft_fixture_step_until_appended(struct raft_fixture *f,
                                     unsigned i,
                                     raft_index index,
                                     unsigned max_msecs)
{
    struct step_apply apply = {i, index};
    return raft_fixture_step_until(f, hasAppendedIndex, &apply, max_msecs);
}

bool raft_fixture_step_until_append_confirmed(struct raft_fixture *f,
					      unsigned i,
					      raft_index index,
					      unsigned max_msecs)
{
	struct step_apply apply = {i, index};
	return raft_fixture_step_until(f, hasConfirmedAppendIndex, &apply, max_msecs);
}

struct step_state
{
    unsigned i;
    int state;
};

static bool hasState(struct raft_fixture *f, void *arg)
{
    struct step_state *target = (struct step_state *)arg;
    struct raft *raft;
    raft = raft_fixture_get(f, target->i);
    return raft_state(raft) == target->state;
}

bool raft_fixture_step_until_state_is(struct raft_fixture *f,
                                      unsigned i,
                                      int state,
                                      unsigned max_msecs)
{
    struct step_state target = {i, state};
    return raft_fixture_step_until(f, hasState, &target, max_msecs);
}

struct step_term
{
    unsigned i;
    raft_term term;
};

static bool hasTerm(struct raft_fixture *f, void *arg)
{
    struct step_term *target = (struct step_term *)arg;
    struct raft *raft;
    raft = raft_fixture_get(f, target->i);
    return raft->current_term == target->term;
}

bool raft_fixture_step_until_term_is(struct raft_fixture *f,
                                     unsigned i,
                                     raft_term term,
                                     unsigned max_msecs)
{
    struct step_term target = {i, term};
    return raft_fixture_step_until(f, hasTerm, &target, max_msecs);
}

struct step_vote
{
    unsigned i;
    unsigned j;
};

static bool hasVotedFor(struct raft_fixture *f, void *arg)
{
    struct step_vote *target = (struct step_vote *)arg;
    struct raft *raft;
    raft = raft_fixture_get(f, target->i);
    return raft->voted_for == target->j + 1;
}

bool raft_fixture_step_until_voted_for(struct raft_fixture *f,
                                       unsigned i,
                                       unsigned j,
                                       unsigned max_msecs)
{
    struct step_vote target = {i, j};
    return raft_fixture_step_until(f, hasVotedFor, &target, max_msecs);
}

struct step_deliver
{
    unsigned i;
    unsigned j;
};

static bool hasDelivered(struct raft_fixture *f, void *arg)
{
    struct step_deliver *target = (struct step_deliver *)arg;
    struct raft *raft;
    struct io *io;
    struct raft_message *message;
    queue *head;
    raft = raft_fixture_get(f, target->i);
    io = raft->io->impl;
    QUEUE_FOREACH(head, &io->requests)
    {
        struct ioRequest *r;
        r = QUEUE_DATA(head, struct ioRequest, queue);
        message = NULL;
        switch (r->type) {
            case SEND:
                message = &((struct send *)r)->message;
                break;
            case TRANSMIT:
                message = &((struct transmit *)r)->message;
                break;
        }
        if (message != NULL && message->server_id == target->j + 1) {
            return false;
        }
    }
    return true;
}

bool raft_fixture_step_until_delivered(struct raft_fixture *f,
                                       unsigned i,
                                       unsigned j,
                                       unsigned max_msecs)
{
    struct step_deliver target = {i, j};
    return raft_fixture_step_until(f, hasDelivered, &target, max_msecs);
}

struct step_send_rv {
	unsigned dst;
	struct raft_request_vote *rv;
};

static bool hasRVForSend(struct raft_fixture *f,
	void *arg)
{
	struct step_send_rv *expect = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)expect->rv->candidate_id);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_REQUEST_VOTE)
				continue;
			if (message->server_id != expect->dst+1)
				continue;

			struct raft_request_vote rv = message->request_vote;
			assert(rv.term == expect->rv->term);
			assert(rv.last_log_index == expect->rv->last_log_index);
			assert(rv.last_log_term == expect->rv->last_log_term);

			return true;
		}
	}

	return false;
}

bool raft_fixture_step_until_rv_for_send(struct raft_fixture *f,
									   	unsigned i,
									   	struct raft_request_vote *rv,
									   	unsigned max_msecs)
{
	struct step_send_rv target = {i, rv};
	return raft_fixture_step_until(f, hasRVForSend, &target, max_msecs);
}

struct step_send_ae {
	unsigned src;
	unsigned dst;
	struct raft_append_entries *ae;
	bool   heartbeat;
};

static bool hasAEForSend(struct raft_fixture *f, void *arg)
{
	struct step_send_ae *expect = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)expect->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_APPEND_ENTRIES)
				continue;
			if (message->server_id != expect->dst+1)
				continue;

			struct raft_append_entries ae = message->append_entries;
			if (ae.term != expect->ae->term)
				continue;
			if (ae.prev_log_index != expect->ae->prev_log_index)
				continue;
			if (ae.prev_log_term != expect->ae->prev_log_term)
				continue;
			if (ae.n_entries != expect->ae->n_entries)
				continue;

			return true;
		}
	}

	return false;
}

bool raft_fixture_step_until_ae_for_send(struct raft_fixture *f,
										unsigned i,
										unsigned j,
										struct raft_append_entries *ae,
										unsigned max_msecs)
{
	struct step_send_ae target = {i, j, ae, false};
	return raft_fixture_step_until(f, hasAEForSend, &target, max_msecs);;
}

struct step_ae_res {
	unsigned src;
	unsigned dst;
	struct raft_append_entries_result *res;
};

static bool hasAEResponse(struct raft_fixture *f, void *arg)
{

	struct step_ae_res *expect = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, expect->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_APPEND_ENTRIES_RESULT)
				continue;
			if (message->server_id != expect->dst+1)
				continue;

			if (!expect->res)
				return true;
			struct raft_append_entries_result res = message->append_entries_result;
			if (res.last_log_index != expect->res->last_log_index)
				continue;
			if (res.rejected != expect->res->rejected)
				continue;
			if (res.term != expect->res->term)
				continue;

			return true;
		}
	}


	return false;
}

bool raft_fixture_step_until_ae_response(struct raft_fixture *f,
										 unsigned i,
										 unsigned j,
										 struct raft_append_entries_result *res,
										 unsigned max_msecs)
{
	struct step_ae_res target = {i, j ,res};
	return raft_fixture_step_until(f, hasAEResponse, &target, max_msecs);
}

struct step_rv_res {
	unsigned src;
	unsigned dst;
	struct raft_request_vote_result *res;
};

static bool hasRVResponse(struct raft_fixture *f,
						  void *arg)
{
	struct step_rv_res *expect = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, expect->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_REQUEST_VOTE_RESULT)
				continue;
			if (message->server_id != expect->dst+1)
				continue;

			struct raft_request_vote_result res = message->request_vote_result;
			assert(res.term == expect->res->term);
			assert(res.vote_granted == expect->res->vote_granted);

			return true;
		}
	}

	return false;
}

bool raft_fixture_step_until_rv_response(struct raft_fixture *f,
										 unsigned i,
										 unsigned j,
										 struct raft_request_vote_result *res,
										 unsigned max_msecs)
{
	struct step_rv_res target = {i, j, res};
	return raft_fixture_step_until(f, hasRVResponse, &target, max_msecs);
}

static bool mockAE(struct raft_fixture *f, void *arg)
{
	struct step_send_ae *mock = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)mock->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_APPEND_ENTRIES)
				continue;
			if (message->server_id != mock->dst+1)
				continue;
			if (mock->heartbeat && message->append_entries.n_entries != 0)
				continue;
			if (!mock->heartbeat && message->append_entries.n_entries == 0)
				continue;

		message->append_entries.term = mock->ae->term;
		message->append_entries.prev_log_index = mock->ae->prev_log_index;
		message->append_entries.prev_log_term = mock->ae->prev_log_term;
		message->append_entries.n_entries = mock->ae->n_entries;
		return true;
		}
	}

	return false;
}

bool raft_fixture_step_ae_mock(struct raft_fixture *f,
									 	unsigned i,
									 	unsigned j,
										struct raft_append_entries *ae)
{
	struct step_send_ae mock = {i, j, ae, false};
	return mockAE(f, &mock);
}

bool raft_fixture_step_heartbeat_mock(struct raft_fixture *f,
	unsigned i,
	unsigned j,
	struct raft_append_entries *ae)
{
	struct step_send_ae mock = {i, j, ae, true};
	return mockAE(f, &mock);
}


struct raft_append_entries *getAE(
	struct raft_fixture *f,
	void *arg)
{
	struct step_send_ae *expect = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)expect->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_APPEND_ENTRIES)
				continue;
			if (message->server_id != expect->dst+1)
				continue;

			struct raft_append_entries ae = message->append_entries;
			if (ae.term != expect->ae->term)
				continue;
			if (ae.prev_log_index != expect->ae->prev_log_index)
				continue;
			if (ae.prev_log_term != expect->ae->prev_log_term)
				continue;
			if (ae.n_entries != expect->ae->n_entries)
				continue;

			return &message->append_entries;
		}
	}
	return NULL;
}

struct raft_append_entries *raft_fixture_get_ae_req(
	struct raft_fixture *f,
	unsigned i,
	unsigned j,
	struct raft_append_entries *ae)
{
	struct step_send_ae mock = {i, j, ae, false};
	return getAE(f, &mock);
}

static bool mockRV(struct raft_fixture *f,
					  void *arg)
{
	struct step_send_rv *mock = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)mock->rv->candidate_id);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_REQUEST_VOTE)
				continue;
			if (message->server_id != mock->dst+1)
				continue;

			message->request_vote.term = mock->rv->term;
			message->request_vote.last_log_index = mock->rv->last_log_index;
			message->request_vote.last_log_term = mock->rv->last_log_term;
			return true;
		}
	}

	return false;
}

bool raft_fixture_step_rv_mock(struct raft_fixture *f,
								unsigned i,
								struct raft_request_vote *rv)
{
	struct step_send_rv mock = {i, rv};
	return mockRV(f, &mock);
}

struct step_install_snapshot {
	unsigned src;
	unsigned dst;
	struct raft_install_snapshot* snap;
};

static bool mockInstallSnapshot(struct raft_fixture *f, void *arg)
{
	struct step_install_snapshot *mock = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)mock->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_INSTALL_SNAPSHOT)
				continue;
			if (message->server_id != mock->dst+1)
				continue;
		message->install_snapshot.term = mock->snap->term;
		return true;
		}
	}

	return false;
}

static bool hasSnapForSend(struct raft_fixture *f, void *arg)
{
	struct step_install_snapshot *mock = arg;
	struct raft *raft;
	struct io *io;
	struct raft_message *message;
	queue *head;
	raft = raft_fixture_get(f, (unsigned)mock->src);
	io = raft->io->impl;
	QUEUE_FOREACH(head, &io->requests)
	{
		struct ioRequest *r;
		r = QUEUE_DATA(head, struct ioRequest, queue);
		message = NULL;
		if (r->type == SEND) {
			message = &((struct send *)r)->message;
			if (!message)
				continue;
			if (message->type != RAFT_IO_INSTALL_SNAPSHOT)
				continue;
			if (message->server_id != mock->dst+1)
				continue;
			if (message->install_snapshot.term != mock->snap->term)
				continue;
		return true;
		}
	}

	return false;
}


bool raft_fixture_step_until_snapshot_for_send(struct raft_fixture *f,
					       unsigned i,
					       unsigned j,
					       struct raft_install_snapshot *snap,
					       unsigned max_msecs)
{
	struct step_install_snapshot mock = {i, j, snap};

	return raft_fixture_step_until(f, hasSnapForSend, &mock, max_msecs);;
}

bool raft_fixture_step_snapshot_mock(struct raft_fixture *f,
				     unsigned i,
				     unsigned j,
				     struct raft_install_snapshot *snap)
{
	struct step_install_snapshot mock = {i, j, snap};

	return mockInstallSnapshot(f, &mock);
}


void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioDisconnect(io1, io2);
}

void raft_fixture_reconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioReconnect(io1, io2);
}

void raft_fixture_saturate(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioSaturate(io1, io2);
}

void disconnectFromAll(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        if (j == i) {
            continue;
        }
        raft_fixture_saturate(f, i, j);
        raft_fixture_saturate(f, j, i);
    }
}

bool raft_fixture_saturated(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    return ioSaturated(io1, io2);
}

void raft_fixture_desaturate(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioDesaturate(io1, io2);
}

void raft_fixture_kill(struct raft_fixture *f, unsigned i)
{
    disconnectFromAll(f, i);
    f->servers[i].alive = false;
}

int raft_fixture_grow(struct raft_fixture *f, struct raft_fsm *fsm)
{
    unsigned i;
    unsigned j;
    int rc;
    i = f->n;
    f->n++;

    rc = serverInit(f, i, fsm);
    if (rc != 0) {
        return rc;
    }

    serverConnectToAll(f, i);
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        ioConnect(io2, io1);
    }

    return 0;
}

void raft_fixture_set_randomized_election_timeout(struct raft_fixture *f,
                                                  unsigned i,
                                                  unsigned msecs)
{
    struct io *io = f->servers[i].io.impl;
    io->randomized_election_timeout = msecs;
}

void raft_fixture_set_network_latency(struct raft_fixture *f,
                                      unsigned i,
                                      unsigned msecs)
{
    struct io *io = f->servers[i].io.impl;
    io->network_latency = msecs;
}

void raft_fixture_set_disk_latency(struct raft_fixture *f,
                                   unsigned i,
                                   unsigned msecs)
{
    struct io *io = f->servers[i].io.impl;
    io->disk_latency = msecs;
}

void raft_fixture_set_term(struct raft_fixture *f, unsigned i, raft_term term)
{
    struct io *io = f->servers[i].io.impl;
    io->term = term;
}

void raft_fixture_set_snapshot(struct raft_fixture *f,
                               unsigned i,
                               struct raft_snapshot *snapshot)
{
    struct io *io = f->servers[i].io.impl;
    io->snapshot = snapshot;
}

void raft_fixture_add_entry(struct raft_fixture *f,
                            unsigned i,
                            struct raft_entry *entry)
{
    struct io *io = f->servers[i].io.impl;
    struct raft_entry *entries;
    entries = raft_realloc(io->entries, (io->n + 1) * sizeof *entries);
    assert(entries != NULL);
    entries[io->n] = *entry;
    io->entries = entries;
    io->n++;
}

void raft_fixture_io_fault(struct raft_fixture *f,
                           unsigned i,
                           int delay,
                           int repeat)
{
    struct io *io = f->servers[i].io.impl;
    io->fault.countdown = delay;
    io->fault.n = repeat;
}

void raft_fixture_io_fault_reset_locations(struct raft_fixture *f, unsigned i)
{
    struct io *io = f->servers[i].io.impl;
    io->fault.mask = RAFT_IOFAULT_ALL;
}

void raft_fixture_io_fault_set_locations(struct raft_fixture *f, unsigned i,
                                         unsigned mask)
{
    struct io *io = f->servers[i].io.impl;
    io->fault.mask = mask;
}

unsigned raft_fixture_n_send(struct raft_fixture *f, unsigned i, int type)
{
    struct io *io = f->servers[i].io.impl;
    return io->n_send[type];
}

unsigned raft_fixture_n_recv(struct raft_fixture *f, unsigned i, int type)
{
    struct io *io = f->servers[i].io.impl;
    return io->n_recv[type];
}

static bool raft_fixture_entry_cmp(const struct raft_entry *dst,
				   const struct raft_entry *src)
{
	assert(dst);
	assert(src);

	return dst->term == src->term
		&& dst->type == src->type
		&& dst->buf.len == src->buf.len
		&& memcmp(dst->buf.base, src->buf.base, dst->buf.len) == 0;
}

bool raft_fixture_log_cmp(struct raft_fixture *f, unsigned i, unsigned j)
{
	struct raft_log *log_i = &f->servers[i].raft.log;
	struct raft_log *log_j = &f->servers[j].raft.log;
	size_t num = logNumEntries(log_i);
	raft_index last_index = logLastIndex(log_i);
	raft_index index;

	if (num != logNumEntries(log_j))
		return false;
	if (last_index != logLastIndex(log_j))
		return false;
	assert(last_index >= num);

	for (index = last_index - num + 1; index <= last_index; ++index) {
		if (!raft_fixture_entry_cmp(logGet(log_i, index),
					    logGet(log_j, index)))
					    return false;
	}
	return true;
}
struct step_commit
{
	unsigned i;
	raft_index index;
};

static bool hasCommittedIndex(struct raft_fixture *f, void *arg)
{
	struct step_commit *commit = (struct step_commit*)arg;
	struct raft *raft;
	unsigned n = 0;
	unsigned i;

	if (commit->i < f->n) {
		raft = raft_fixture_get(f, commit->i);
		return raft->commit_index >= commit->index;
	}

	for (i = 0; i < f->n; i++) {
		raft = raft_fixture_get(f, i);
		if (raft->commit_index >= commit->index) {
			n++;
		}
	}
	return n == f->n;
}

bool raft_fixture_step_until_committed(struct raft_fixture *f,
	unsigned i,
	raft_index index,
	unsigned max_msecs)
{
	struct step_commit commit = {i, index};
	return raft_fixture_step_until(f, hasCommittedIndex, &commit, max_msecs);
}

void raft_fixture_mock_errno(int errno)
{
	g_fixture_errno = errno;
}

int raft_fixture_construct_configuration_log_buf(unsigned n_server,
												unsigned n_voter,
												struct raft_entry *et)
{
	int ret = 0;
	struct raft_configuration conf;
	struct raft_buffer buf;

	//construct configuration that contain  n servers and the
	//first n_voter servers wille be set as voter
	ret = raft_fixture_construct_configuration(n_server, n_voter, &conf);
	assert(ret == 0);

	//encode and copy buf
	configurationEncode(&conf, &buf);
	assert(ret == 0);
	et->buf = buf;

	raft_configuration_close(&conf);
	return 0;
}

int raft_fixture_construct_configuration(unsigned n_server,
										unsigned n_voter,
										struct raft_configuration *conf)
{

    raft_configuration_init(conf);
    for (unsigned id = 1; id <= n_server; id++) {
        int role = id <= n_voter ? RAFT_VOTER : RAFT_STANDBY;
        int rv;

	rv = raft_configuration_add(conf, id, role);
        if (rv != 0) {
            return rv;
        }
    }

	return 0;
}

bool raft_fixture_promotable(struct raft_configuration *conf, unsigned id)
{
	unsigned  i;
	struct raft_server *server;

	for (i = 0; i < conf->n; ++i) {
		server = &conf->servers[i];
		if (server->id != id)
			continue;
		return server->role == RAFT_VOTER;
	}

	return false;
}

struct step_phase {
    unsigned int i;
    int phase;
};

static bool in_phase(struct raft_fixture *f, void *arg)
{
    struct step_phase *sp = (struct step_phase *)arg;
    struct raft *raft;

    if (sp->i < f->n) {
        raft = raft_fixture_get(f, sp->i);
        return (int)raft->configuration.phase == sp->phase;
    }

    return false;
}

void raft_fixture_step_until_phase(struct raft_fixture *f, unsigned int i, int phase, unsigned msecs)
{
    struct step_phase sp;
    sp.i = i;
    sp.phase = phase;
    raft_fixture_step_until(f, in_phase, &sp, msecs);
}

raft_index raft_fixture_last_index(struct raft_fixture *f, unsigned int i)
{
    struct raft *r;

    r = raft_fixture_get(f, i);
    return logLastIndex(&r->log);
}

static void raft_fixture_record(void *data, enum raft_event_level level,
                                const char* fn, const char *file, int line,
                                const char *fmt, ...)
{
    (void)level;
    (void)fn;
    struct raft_fixture *f = data;
    char buf[1024];
    va_list va;

    va_start(va, fmt);
    vsnprintf(buf, sizeof(buf), fmt, va);
    va_end(va);
    fprintf(stdout, "[event][%6llu] %30s:%*d : %s\n", f->time, file, 3, line,
            buf);
}

static bool raft_fixture_id_allowed(void *data, raft_id id)
{
    (void)data;
    (void)id;

    return true;
}

static int raft_fixture_get_level(void *data)
{
    (void)data;

    return RAFT_DEBUG;
}

void raft_fixture_enable_recorder(struct raft_fixture *f)
{
    f->recorder.data   = f;
    f->recorder.is_id_allowed = raft_fixture_id_allowed;
    f->recorder.record = raft_fixture_record;
    f->recorder.get_level = raft_fixture_get_level;

    raft_set_event_recorder(&f->recorder);
}

struct step_io_fault {
    unsigned i;
    int fault;
};

static bool hasIOFault(struct raft_fixture *f, void *arg)
{
    struct step_io_fault *expect = arg;
    struct raft *raft;
	struct io *io;
	raft = raft_fixture_get(f, (unsigned)expect->i);
	io = raft->io->impl;

    return io->fault.n == expect->fault;
}

bool raft_fixture_step_until_io_fault(struct raft_fixture *f,
									  unsigned i,
									  int n,
									  unsigned max_msecs)
{
	struct step_io_fault target = {i, n};
	return raft_fixture_step_until(f, hasIOFault, &target, max_msecs);
}

#undef tracef
