#ifndef RAFT_H
#define RAFT_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#define RAFT_API __attribute__((visibility("default")))

/**
 * Error codes.
 */
#define RAFT_NOMEM 1            /* Out of memory */
#define RAFT_BADID 2            /* Server ID is not valid */
#define RAFT_DUPLICATEID 3      /* Server ID already in use */
#define RAFT_DUPLICATEADDRESS 4 /* Server address already in use */
#define RAFT_BADROLE 5          /* Server role is not valid */
#define RAFT_MALFORMED 6
#define RAFT_NOTLEADER 7
#define RAFT_LEADERSHIPLOST 8
#define RAFT_SHUTDOWN 9
#define RAFT_CANTBOOTSTRAP 10
#define RAFT_CANTCHANGE 11
#define RAFT_CORRUPT 12
#define RAFT_CANCELED 13
#define RAFT_NAMETOOLONG 14
#define RAFT_TOOBIG 15
#define RAFT_NOCONNECTION 16
#define RAFT_BUSY 17
#define RAFT_IOERR 18        /* File system or storage error */
#define RAFT_NOTFOUND 19     /* Resource not found */
#define RAFT_INVALID 20      /* Invalid parameter */
#define RAFT_UNAUTHORIZED 21 /* No access to a resource */
#define RAFT_NOSPACE 22      /* Not enough space on disk */
#define RAFT_TOOMANY 23      /* Some system or raft limit was hit */
#define RAFT_RETRY 24

/**
 * Size of human-readable error message buffers.
 */
#define RAFT_ERRMSG_BUF_SIZE 256

/**
 * Bits used for packet id
 */
#define RAFT_PKT_BITS 48

/**
 * Return the error message describing the given error code.
 */
RAFT_API const char *raft_strerror(int errnum);

typedef unsigned long long raft_id;

/**
 * Hold the value of a raft term. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long raft_term;

/**
 * Hold the value of a raft entry index. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long raft_index;

/**
 * Hold a time value expressed in milliseconds since the epoch.
 */
typedef unsigned long long raft_time;

/**
 * A data buffer.
 */
struct raft_buffer
{
    void *base; /* Pointer to the buffer data. */
    size_t len; /* Length of the buffer. */
};

/**
 * Server role codes.
 */

#define RAFT_STANDBY 0 /* Replicate log, does not participate in quorum. */
#define RAFT_VOTER 1   /* Replicate log, does participate in quorum. */
#define RAFT_SPARE 2   /* Does not replicate log, or participate in quorum. */
#define RAFT_LOGGER 3  /* Replicate log, does participate in quorum, no fsm */

/**
 * Server group types.
 */
enum raft_group {
    RAFT_GROUP_OLD = 0x01,
    RAFT_GROUP_NEW = 0x02,
    RAFT_GROUP_ANY = 0x03,
};
/**
 * Hold information about a single server in the cluster configuration.
 */
struct raft_server
{
    raft_id id;    /* Server ID, must be greater than zero. */
    int role;      /* Server role. */
    int role_new;  /* Server role in new group. */
    int group;     /* Server group. */
};

enum raft_conf_phase {
    RAFT_CONF_NORMAL = 0,
    RAFT_CONF_JOINT,
};

/**
 * Hold information about all servers currently part of the cluster.
 */
struct raft_configuration
{
    struct raft_server *servers; /* Array of servers member of the cluster. */
    unsigned n;                  /* Number of servers in the array. */
    enum raft_conf_phase phase;
};

/**
 * Initialize an empty raft configuration.
 */
RAFT_API void raft_configuration_init(struct raft_configuration *c);

/**
 * Release all memory used by the given configuration object.
 */
RAFT_API void raft_configuration_close(struct raft_configuration *c);

/**
 * Add a server to a raft configuration.
 *
 * The @id must be greater than zero and @address point to a valid string.
 *
 * The @role must be either #RAFT_VOTER, #RAFT_STANDBY, #RAFT_SPARE.
 *
 * If @id or @address are already in use by another server in the configuration,
 * an error is returned.
 *
 * The @address string will be copied and can be released after this function
 * returns.
 */
RAFT_API int raft_configuration_add(struct raft_configuration *c,
                                    raft_id id,
                                    int role);

/**
 * Encode the given configuration object.
 *
 * The memory of the returned buffer is allocated using raft_malloc(), and
 * client code is responsible for releasing it when no longer needed.
 */
RAFT_API int raft_configuration_encode(const struct raft_configuration *c,
                                       struct raft_buffer *buf);

/**
 * Return voter number in configuration.
 */
RAFT_API unsigned raft_configuration_voter_count(
                                            const struct raft_configuration *c);

/**
 * Hash function which outputs a 64-bit value based on a text and a number.
 *
 * This can be used to generate a unique ID for a new server being added, for
 * example based on its address and on the current time in milliseconds since
 * the Epoch.
 *
 * It's internally implemented as a SHA1 where only the last 8 bytes of the hash
 * value are kept.
 */
RAFT_API unsigned long long raft_digest(const char *text, unsigned long long n);

/**
 * Log entry types.
 */
enum {
    RAFT_COMMAND = 1, /* Command for the application FSM. */
    RAFT_BARRIER,     /* Wait for all previous commands to be applied. */
    RAFT_CHANGE       /* Raft configuration change. */
};

/**
 * A single entry in the raft log.
 *
 * An entry that originated from this raft instance while it was the leader
 * (typically via client calls to raft_apply()) should normally have a @buf
 * attribute referencing directly the memory that was originally allocated by
 * the client itself to contain the entry data, and the @batch attribute set to
 * #NULL.
 *
 * An entry that was received from the network as part of an AppendEntries RPC
 * or that was loaded from disk at startup should normally have a @batch
 * attribute that points to a contiguous chunk of memory that contains the data
 * of the entry itself plus possibly the data for other entries that were
 * received or loaded with it at the same time. In this case the @buf pointer
 * will be equal to the @batch pointer plus an offset, that locates the position
 * of the entry's data within the batch.
 *
 * When the @batch attribute is not #NULL the raft library will take care of
 * releasing that memory only once there are no more references to the
 * associated entries.
 *
 * This arrangement makes it possible to minimize the amount of memory-copying
 * when performing I/O.
 */
struct raft_entry
{
    raft_term term;         /* Term in which the entry was created. */
    unsigned short type;    /* Type (FSM command, barrier, config change). */
    struct raft_buffer buf; /* Entry data. */
    void *batch;            /* Batch that buf's memory points to, if any. */
};

/**
 * Counter for outstanding references to a log entry.
 *
 * When an entry is first appended to the log, its refcount is set to one (the
 * log itself is the only one referencing the entry). Whenever an entry is
 * included in an I/O request (to write it to disk or to send it to other
 * servers) its refcount is increased by one. Whenever an entry gets deleted
 * from the log its refcount is decreased by one. Likewise, whenever an I/O
 * request is completed the refcount of the relevant entries is decreased by
 * one. When the refcount drops to zero the memory that its @buf attribute
 * points to gets released, or, if the @batch attribute is non-NULL, a check is
 * made to see if all other entries of the same batch also have a zero refcount,
 * and the memory that @batch points to gets released if that's the case.
 */
struct raft_entry_ref
{
    raft_term term;              /* Term of the entry being ref-counted. */
    raft_index index;            /* Index of the entry being ref-counted. */
    unsigned short count;        /* Number of references. */
    struct raft_entry_ref *next; /* Next item in the bucket (for collisions). */
};

/**
 * In-memory cache of the persistent raft log stored on disk.
 *
 * The raft log cache is implemented as a circular buffer of log entries, which
 * makes some frequent operations very efficient (e.g. deleting the first N
 * entries when snapshotting).
 */
struct raft_log
{
    struct raft_entry *entries;  /* Circular buffer of log entries. */
    size_t size;                 /* Number of available slots in the buffer. */
    size_t front, back;          /* Indexes of used slots [front, back). */
    raft_index offset;           /* Index of first entry is offset+1. */
    struct raft_entry_ref *refs; /* Log entries reference counts hash table. */
    size_t refs_size;            /* Size of the reference counts hash table. */
    struct                       /* Information about last snapshot, or zero. */
    {
        raft_index last_index; /* Snapshot replaces all entries up to here. */
        raft_term last_term;   /* Term of last index. */
    } snapshot;
    struct raft_log_hook *hook;  /* Hook functions for log.  */
};

/**
 * Hold the arguments of a RequestVote RPC.
 *
 * The RequestVote RPC is invoked by candidates to gather votes.
 */
struct raft_request_vote
{
    raft_term term;            /* Candidate's term. */
    raft_id candidate_id;      /* ID of the server requesting the vote. */
    raft_index last_log_index; /* Index of candidate's last log entry. */
    raft_term last_log_term;  /* Term of log entry at last_log_index. */
    bool disrupt_leader;       /* True if current leader should be discarded. */
    bool pre_vote;             /* True if this is a pre-vote request. */
};

/**
 * Hold the result of a RequestVote RPC.
 */
struct raft_request_vote_result
{
    raft_term term;    /* Receiver's current term (candidate updates itself). */
    bool vote_granted; /* True means candidate received vote. */
    bool pre_vote;
};

/**
 * Hold the arguments of an AppendEntries RPC.
 *
 * The AppendEntries RPC is invoked by the leader to replicate log entries. It's
 * also used as heartbeat (figure 3.1).
 */
struct raft_append_entries
{
    raft_index  pkt;
    raft_term term;             /* Leader's term. */
    raft_index prev_log_index;  /* Index of log entry preceeding new ones. */
    raft_term prev_log_term;    /* Term of entry at prev_log_index. */
    raft_index leader_commit;   /* Leader's commit index. */
    struct raft_entry *entries; /* Log entries to append. */
    unsigned n_entries;         /* Size of the log entries array. */
    raft_index snapshot_index;  /* Index of current snapshot */
    unsigned trailing;
};

/**
 * Hold the result of an AppendEntries RPC (figure 3.1).
 */
struct raft_append_entries_result
{
    raft_index pkt;
    raft_term term;            /* Receiver's current_term. */
    raft_index rejected;       /* If non-zero, the index that was rejected. */
    raft_index last_log_index; /* Receiver's last log entry index, as hint. */
};

/**
 * Hold the arguments of an InstallSnapshot RPC (figure 5.3).
 */
struct raft_install_snapshot
{
    raft_term term;                 /* Leader's term. */
    raft_index last_index;          /* Index of last entry in the snapshot. */
    raft_term last_term;            /* Term of last_index. */
    struct raft_configuration conf; /* Config as of last_index. */
    raft_index conf_index;          /* Commit index of conf. */
    struct raft_buffer data;        /* Raw snapshot data. */
};

/**
 * Hold the arguments of a TimeoutNow RPC.
 *
 * The TimeoutNow RPC is invoked by leaders to transfer leadership to a
 * follower.
 */
struct raft_timeout_now
{
    raft_term term;            /* Leader's term. */
    raft_index last_log_index; /* Index of leader's last log entry. */
    raft_term last_log_term;  /* Term of log entry at last_log_index. */
};

/**
 * Type codes for RPC messages.
 */
enum {
    RAFT_IO_APPEND_ENTRIES = 1,
    RAFT_IO_APPEND_ENTRIES_RESULT,
    RAFT_IO_REQUEST_VOTE,
    RAFT_IO_REQUEST_VOTE_RESULT,
    RAFT_IO_INSTALL_SNAPSHOT,
    RAFT_IO_TIMEOUT_NOW
};

/**
 * A single RPC message that can be sent or received over the network.
 */
struct raft_message
{
    unsigned short type;        /* RPC type code. */
    raft_id server_id;          /* ID of sending or destination server. */
    union {                     /* Type-specific data */
        struct raft_request_vote request_vote;
        struct raft_request_vote_result request_vote_result;
        struct raft_append_entries append_entries;
        struct raft_append_entries_result append_entries_result;
        struct raft_install_snapshot install_snapshot;
        struct raft_timeout_now timeout_now;
    };
};

/**
 * Hold the details of a snapshot.
 */
struct raft_snapshot
{
    /* Index and term of last entry included in the snapshot. */
    raft_index index;
    raft_term term;

    /* Last committed configuration included in the snapshot, along with the
     * index it was committed at. */
    struct raft_configuration configuration;
    raft_index configuration_index;

    /* Content of the snapshot. When a snapshot is taken, the user FSM can fill
     * the bufs array with more than one buffer. When a snapshot is restored,
     * there will always be a single buffer. */
    struct raft_buffer *bufs;
    unsigned n_bufs;
};

/**
 * Asynchronous request to send an RPC message.
 */
struct raft_io_send;
typedef void (*raft_io_send_cb)(struct raft_io_send *req, int status);
struct raft_io_send
{
    void *data;         /* User data */
    raft_io_send_cb cb; /* Request callback */
};

/**
 * Asynchronous request to store new log entries.
 */
struct raft_io_append;
typedef void (*raft_io_append_cb)(struct raft_io_append *req, int status);
struct raft_io_append
{
    void *data;           /* User data */
    raft_io_append_cb cb; /* Request callback */
    raft_index index;
};

/**
 * Asynchronous request to store a new snapshot.
 */
struct raft_io_snapshot_put;
typedef void (*raft_io_snapshot_put_cb)(struct raft_io_snapshot_put *req,
                                        int status);
struct raft_io_snapshot_put
{
    void *data;                 /* User data */
    raft_io_snapshot_put_cb cb; /* Request callback */
};

/**
 * Asynchronous request to load the most recent snapshot available.
 */
struct raft_io_snapshot_get;
typedef void (*raft_io_snapshot_get_cb)(struct raft_io_snapshot_get *req,
                                        struct raft_snapshot *snapshot,
                                        int status);
struct raft_io_snapshot_get
{
    void *data;                 /* User data */
    raft_io_snapshot_get_cb cb; /* Request callback */
};

/**
 * Asynchronous request to store term and vote.
 */
struct raft_io_set_meta;
typedef void (*raft_io_set_meta_cb)(struct raft_io_set_meta *req, int status);
struct raft_io_set_meta
{
        void *data;           /* User data */
        raft_io_set_meta_cb cb; /* Request callback */
};

struct raft_load_data {
    raft_term term;
    raft_id voted_for;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    raft_index applied_index;
};
/**
 * Asynchronous request to load raft data from the disk.
 */
struct raft_io_load;
typedef void (*raft_io_load_cb)(struct raft_io_load *req,
                                struct raft_load_data *load,
                                int status);
struct raft_io_load
{
    void *data;         /* User data */
    raft_io_load_cb cb; /* Request callback */
};

struct raft_io_bootstrap;
typedef void (*raft_io_bootstrap_cb)(struct raft_io_bootstrap *req,
                                     int status);
struct raft_io_bootstrap
{
    void *data;         	 /* User data */
    raft_io_bootstrap_cb cb; /* Request callback */
};
/**
 * Customizable tracer, for debugging purposes.
 */
struct raft_tracer
{
    /**
     * Implementation-defined state object.
     */
    void *impl;

    /**
     * Emit the given trace message, possibly decorating it with the provided
     * metadata.
     */
    void (*emit)(struct raft_tracer *t,
                 const char *file,
                 int line,
                 const char *message);
};

struct raft_io; /* Forward declaration. */

/**
 * Callback invoked by the I/O implementation at regular intervals.
 */
typedef void (*raft_io_tick_cb)(struct raft_io *io);

/**
 * Callback invoked by the I/O implementation when an RPC message is received.
 */
typedef void (*raft_io_recv_cb)(struct raft_io *io, struct raft_message *msg);

typedef void (*raft_io_close_cb)(struct raft_io *io);

struct raft_io
{
    int version;
    void *data;
    void *impl;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int (*init)(struct raft_io *io, raft_id id);
    void (*close)(struct raft_io *io, bool clean, raft_io_close_cb cb);
    int (*load)(struct raft_io *io,
                raft_term *term,
                raft_id *voted_for,
                struct raft_snapshot **snapshot,
                raft_index *start_index,
                struct raft_entry *entries[],
                size_t *n_entries);
    int (*aload)(struct raft_io *io,
                 struct raft_io_load *req,
                 raft_io_load_cb cb);
    int (*start)(struct raft_io *io,
                 unsigned msecs,
                 raft_io_tick_cb tick,
                 raft_io_recv_cb recv);
    int (*bootstrap)(struct raft_io *io, const struct raft_configuration *conf);
    int (*abootstrap)(struct raft_io *io,
              struct raft_io_bootstrap *req,
              const struct raft_configuration *conf,
              raft_io_bootstrap_cb cb);
    int (*recover)(struct raft_io *io, const struct raft_configuration *conf);
    int (*set_meta)(struct raft_io *io,
                    struct raft_io_set_meta *req,
                    raft_term term,
                    raft_id vote,
                    raft_io_set_meta_cb cb);
    int (*send)(struct raft_io *io,
                struct raft_io_send *req,
                const struct raft_message *message,
                raft_io_send_cb cb);
    int (*append)(struct raft_io *io,
                  struct raft_io_append *req,
                  const struct raft_entry entries[],
                  unsigned n,
                  raft_io_append_cb cb);
    int (*truncate)(struct raft_io *io, raft_index index);
    int (*snapshot_put)(struct raft_io *io,
                        unsigned trailing,
                        struct raft_io_snapshot_put *req,
                        const struct raft_snapshot *snapshot,
                        raft_io_snapshot_put_cb cb);
    int (*snapshot_get)(struct raft_io *io,
                        struct raft_io_snapshot_get *req,
                        raft_io_snapshot_get_cb cb);
    raft_time (*time)(struct raft_io *io);
    int (*random)(struct raft_io *io, int min, int max);
#define RAFT_IO_AVAILABLE	0
#define RAFT_IO_BUSY		1
    /*
     * state of this raft I/O instance,
     */
    unsigned short state;
};

struct raft_fsm_apply;
typedef void (*raft_fsm_apply_cb)(struct raft_fsm_apply *req,
                                  void *result,
                                  int status);
struct raft_fsm_apply {
        void *data;
        raft_fsm_apply_cb cb;
};

struct raft_fsm
{
    int version;
    void *data;
    int (*apply)(struct raft_fsm *fsm,
                 struct raft_fsm_apply *req,
                 const struct raft_buffer *buf,
                 raft_fsm_apply_cb cb);
    int (*snapshot)(struct raft_fsm *fsm,
                    struct raft_buffer *bufs[],
                    unsigned *n_bufs);
    int (*restore)(struct raft_fsm *fsm, struct raft_buffer *buf);
};

/**
 * State codes.
 */
enum { RAFT_UNAVAILABLE, RAFT_FOLLOWER, RAFT_CANDIDATE, RAFT_LEADER };

/**
 * Used by leaders to keep track of replication progress for each server.
 */
struct raft_progress
{
    unsigned short state;         /* Probe, pipeline or snapshot. */
    raft_index next_index;        /* Next entry to send. */
    raft_index match_index;       /* Highest index reported as replicated. */
    raft_index snapshot_index;    /* Last index of most recent snapshot sent. */
    raft_time last_send;          /* Timestamp of last AppendEntries RPC. */
    raft_time snapshot_last_send; /* Timestamp of last InstallSnaphot RPC. */
    bool recent_recv;             /* A msg was received within election timeout. */
    raft_time recent_recv_time;   /* Timestamp of last AppendEntriesResult RPC.*/
    raft_time recent_match_time;  /* Timestamp of last matched AppendEntriesResult RPC.*/
};

struct raft; /* Forward declaration. */

/**
 * Close callback.
 *
 * It's safe to release the memory of a raft instance only after this callback
 * has fired.
 */
typedef void (*raft_close_cb)(struct raft *raft);

typedef void (*raft_state_change_cb)(struct raft *raft, int state);

#define RAFT_TICK_STEPDOWN 1

typedef void (*raft_leader_stepdown_cb)(struct raft *raft, int reason);

struct raft_change;   /* Forward declaration */
struct raft_transfer; /* Forward declaration */

/* Quorum types */
enum raft_quorum {
	RAFT_MAJORITY = 0,
	RAFT_FULL
};

/* Abstract request type */
struct request
{
    /* Must be kept in sync with RAFT__REQUEST in raft.h */
    void *data;
    raft_time time;
    int type;
    raft_index index;
    void *queue[2];
};

struct request_slot
{
    void *req;
    raft_index index;
};

struct request_registry
{
    struct request_slot *slots; // Circular buffer of request
    size_t size;                // Number of slots
    size_t front, back;         // Used slots [front, back)
};

struct raft_snapshot_sample {
	raft_index index;
};

struct raft_snapshot_sampler {
	raft_time span; // Time span between first and last sample
	raft_time period; // Sample period in ms
	struct raft_snapshot_sample *samples; // Circular buffer of sample
	size_t			     size; // Number of samples
	size_t			     last; // Last sample index
	raft_time		     last_time;
};

struct raft_log_hook {
	void *data;
	/**
     * Called after log close.
     */
	void (*log_close)(struct raft_log_hook *h);
	/**
     * Called after entry add to queue.
     */
	void (*entry_add)(struct raft_log_hook *h, const struct raft_entry *entry,
			  raft_index index);
	/**
     * Called after entry remove from queue.
     */
	void (*entry_remove)(struct raft_log_hook *h,
                         const struct raft_entry *entry, raft_index index);
	/**
     * Called before release entry's buf. the entry is still in the queue.
     */
	void (*entry_release)(struct raft_log_hook *h,
                          const struct raft_entry *entry, raft_index index);
};

/**
 * Hold and drive the state of a single raft server in a cluster.
 */
struct raft
{
    void *data;                 /* Custom user data. */
    struct raft_tracer *tracer; /* Tracer implementation. */
    struct raft_io *io;         /* Disk and network I/O implementation. */
    struct raft_fsm *fsm;       /* User-defined FSM to apply commands to. */
    raft_id id;                 /* Server ID of this raft instance. */
    int     role;               /* Server role of this raft instance. */
    /*
     * Cache of the server's persistent state, updated on stable storage before
     * responding to RPCs (Figure 3.1).
     */
    raft_term current_term; /* Latest term server has seen. */
    raft_id voted_for;      /* Candidate that received vote in current term. */
    struct raft_log log;    /* Log entries. */

    /*
     * Current membership configuration (Chapter 4).
     *
     * At any given moment the current configuration can be committed or
     * uncommitted.
     *
     * If a server is voting, the log entry with index 1 must always contain the
     * first committed configuration.
     *
     * The possible scenarios are:
     *
     * 1. #configuration_index and #configuration_uncommitted_index are both
     *    zero. This should only happen when a brand new server starts joining a
     *    cluster and is waiting to receive log entries from the current
     *    leader. In this case #configuration must be empty and have no servers.
     *
     * 2. #configuration_index is non-zero while #configuration_uncommitted_index
     *    is zero. In this case the content of #configuration must match the one
     *    of the log entry at #configuration_index.
     *
     * 3. #configuration_index and #configuration_uncommitted_index are both
     *    non-zero, with the latter being greater than the former. In this case
     *    the content of #configuration must match the one of the log entry at
     *    #configuration_uncommitted_index.
     */
    struct raft_configuration configuration;
    raft_index configuration_index;
    raft_index configuration_uncommitted_index;

    /*
     * Election timeout in milliseconds (default 1000).
     *
     * From 3.4:
     *
     *   Raft uses a heartbeat mechanism to trigger leader election. When
     *   servers start up, they begin as followers. A server remains in follower
     *   state as long as it receives valid RPCs from a leader or
     *   candidate. Leaders send periodic heartbeats (AppendEntries RPCs that
     *   carry no log entries) to all followers in order to maintain their
     *   authority. If a follower receives no communication over a period of
     *   time called the election timeout, then it assumes there is no viable
     *   leader and begins an election to choose a new leader.
     *
     * This is the baseline value and will be randomized between 1x and 2x.
     *
     * See raft_change_election_timeout() to customize the value of this
     * attribute.
     */
    unsigned election_timeout;
    unsigned reset_trailing_timeout;
    /*
     * Heartbeat timeout in milliseconds (default 100). This is relevant only
     * for when the raft instance is in leader state: empty AppendEntries RPCs
     * will be sent if this amount of milliseconds elapses without any
     * user-triggered AppendEntries RCPs being sent.
     *
     * From Figure 3.1:
     *
     *   [Leaders] Send empty AppendEntries RPC during idle periods to prevent
     *   election timeouts.
     */
    unsigned heartbeat_timeout;

    /*
     * When the leader sends an InstallSnapshot RPC to a follower it will consider
     * the RPC as failed after this timeout and retry.
     */
    unsigned install_snapshot_timeout;

    /*
     * The fields below hold the part of the server's volatile state which is
     * always applicable regardless of the whether the server is follower,
     * candidate or leader (Figure 3.1). This state is rebuilt automatically
     * after a server restart.
     */
    raft_index commit_index; /* Highest log entry known to be committed */
    raft_index last_applying;/* Highest log entry being applied to the FSM */
    raft_index last_applied; /* Highest log entry applied to the FSM */
    raft_index last_stored;  /* Highest log entry persisted on disk */
    unsigned nr_applying;    /* In-progress applying request */
    int       apply_status;  /* Last apply status */

    /*
     * Current server state of this raft instance, along with a union defining
     * state-specific values.
     */
    unsigned short state;
    union {
        struct /* Follower */
        {
            unsigned randomized_election_timeout; /* Timer expiration. */
            struct                                /* Current leader info. */
            {
                raft_id id;
                raft_index snapshot_index;
                unsigned int trailing;
            } current_leader;
        } follower_state;
        struct
        {
            unsigned randomized_election_timeout; /* Timer expiration. */
            bool *votes;                          /* Vote results. */
            bool disrupt_leader;                  /* For leadership transfer */
            bool in_pre_vote;                     /* True in pre-vote phase. */
        } candidate_state;
        struct
        {
            struct raft_progress *progress; /* Per-server replication state. */
            struct raft_change *change;     /* Pending membership change. */
            raft_id promotee_id;            /* ID of server being promoted. */
            raft_id remove_id;              /* ID of server being removed. */
            int promotee_role;
            unsigned short round_number;    /* Current sync round. */
            raft_index round_index;         /* Target of the current round. */
            raft_time round_start;          /* Start of current round. */
            struct request_registry reg;    /* Outstanding client requests. */
            raft_index min_sync_match_index;/* The minimum sync match index. */
            raft_index min_sync_match_replica; /* The minimum sync replica. */
            /* Replica between min and max timeout*/
            unsigned short replica_sync_between_min_max_timeout;
            bool removed_from_cluster;         /* Removed from cluster */
        } leader_state;
    };

    /* Election timer start.
     *
     * This timer has different purposes depending on the state. Followers
     * convert to candidate after the randomized election timeout has elapsed
     * without leader contact. Candidates start a new election after the
     * randomized election timeout has elapsed without a winner. Leaders step
     * down after the election timeout has elapsed without contacting a majority
     * of voting servers. */
    raft_time election_timer_start;

    /* In-progress leadership transfer request, if any. */
    struct raft_transfer *transfer;

    /*
     * Information about the last snapshot that was taken (if any).
     */
    struct
    {
        unsigned threshold;              /* N. of entries before snapshot */
        unsigned trailing;               /* N. of trailing entries to retain */
        struct raft_snapshot pending;    /* In progress snapshot */
        struct raft_io_snapshot_put put; /* Store snapshot request */
        struct raft_configuration configuration;
    } snapshot;

    raft_state_change_cb state_change_cb;

    raft_leader_stepdown_cb stepdown_cb;
    /*
     * Callback to invoke once a close request has completed.
     */
    raft_close_cb close_cb;

    /*
     * Human-readable message providing diagnostic information about the last
     * error occurred.
     */
    char errmsg[RAFT_ERRMSG_BUF_SIZE];

    /* Whether to use pre-vote to avoid disconnected servers disrupting the
     * current leader, as described in 4.2.3 and 9.6. */
    bool pre_vote;

    /* Limit how long to wait for a stand-by to catch-up with the log when its
     * being promoted to voter. */
    unsigned max_catch_up_rounds;
    unsigned max_catch_up_round_duration;

    /* Threshold for log count per AE message */
    unsigned message_log_threshold;
    /* The limit for unconfirmed log in pipeline mode*/
    unsigned inflight_log_threshold;

    /* Hook implementation */
    struct raft_hook *hook;
    /* Auxiliary information */
    union {
	    struct {
		    bool match_leader;
	    } follower_aux;
    };
    bool sync_replication;
    bool sync_snapshot;
    unsigned sync_replica_timeout_min;
    unsigned sync_replica_timeout_max;

    /* Fields for cope with append */
    unsigned nr_appending_requests;
    int prev_append_status;

    /* Quorum type */
    enum raft_quorum quorum;

    /* nonvoter grant vote */
    bool non_voter_grant_vote;
    /* Flag for enable hook for request */
    bool enable_request_hook;

    bool enable_election_at_start;
    /* Flag for raft dynamic change log trailing */
    bool enable_dynamic_trailing;
    unsigned max_dynamic_trailing;
    struct raft_snapshot_sampler sampler;
    raft_index pkt_id;
    raft_time latest_entry_time;
    struct {
	    bool     enable;    /* Flag for enable aggressive snapshot */
	    unsigned threshold; /* N. of entries before snapshot */
	    unsigned trailing;  /* N. of trailing entries to retain */
    } aggressive_snapshot;
    bool enable_change_cb_on_match;
};

RAFT_API int raft_init(struct raft *r,
                       struct raft_io *io,
                       struct raft_fsm *fsm,
                       raft_id id);

RAFT_API void raft_close(struct raft *r, bool clean, raft_close_cb cb);

/**
 * Bootstrap this raft instance using the given configuration. The instance must
 * not have been started yet and must be completely pristine, otherwise
 * #RAFT_CANTBOOTSTRAP will be returned.
 */
RAFT_API int raft_bootstrap(struct raft *r,
                            const struct raft_configuration *conf);
/**
 * Bootstrap this raft asynchronously
 */
RAFT_API int raft_abootstrap(struct raft *r,
                 struct raft_io_bootstrap *req,
                 const struct raft_configuration *conf,
                 raft_io_bootstrap_cb cb);
/**
 * Force a new configuration in order to recover from a loss of quorum where the
 * current configuration cannot be restored, such as when a majority of servers
 * die at the same time.
 *
 * This works by appending the new configuration directly to the log stored on
 * disk.
 *
 * In order for this operation to be safe you must follow these steps:
 *
 * 1. Make sure that no servers in the cluster are running, either because they
 *    died or because you manually stopped them.
 *
 * 2. Run @raft_recover exactly one time, on the non-dead server which has
 *    the highest term and the longest log.
 *
 * 3. Copy the data directory of the server you ran @raft_recover on to all
 *    other non-dead servers in the cluster, replacing their current data
 *    directory.
 *
 * 4. Restart all servers.
 */
RAFT_API int raft_recover(struct raft *r,
                          const struct raft_configuration *conf);

RAFT_API int raft_start(struct raft *r);

struct raft_start;
typedef void (*raft_start_cb)(struct raft_start *req, int status);
struct raft_start
{
    void *data;       /* User data */
    raft_start_cb cb; /* Request callback */
};
RAFT_API int raft_astart(struct raft *r,
             struct raft_start *req,
             raft_start_cb cb);

RAFT_API int restoreEntries(struct raft *r,
                   raft_index snapshot_index,
                   raft_term snapshot_term,
                   raft_index start_index,
                   struct raft_entry *entries,
                   size_t n);
/**
 * Set the election timeout.
 *
 * Every raft instance is initialized with a default election timeout of 1000
 * milliseconds. If you wish to tweak it, call this function before starting
 * your event loop.
 *
 * From Chapter 9:
 *
 *   We recommend a range that is 10-20 times the one-way network latency, which
 *   keeps split votes rates under 40% in all cases for reasonably sized
 *   clusters, and typically results in much lower rates.
 *
 * Note that the current random election timer will be reset and a new one timer
 * will be generated.
 */
RAFT_API void raft_set_election_timeout(struct raft *r, unsigned msecs);

/**
 * Set the heartbeat timeout.
 */
RAFT_API void raft_set_heartbeat_timeout(struct raft *r, unsigned msecs);

/**
 * Set the snapshot install timeout.
 */
RAFT_API void raft_set_install_snapshot_timeout(struct raft *r, unsigned msecs);

/**
 * Number of outstanding log entries before starting a new snapshot. The default
 * is 1024.
 */
RAFT_API void raft_set_snapshot_threshold(struct raft *r, unsigned n);

/**
 * Enable or disable pre-vote support. Pre-vote is turned off by default.
 */
RAFT_API void raft_set_pre_vote(struct raft *r, bool enabled);

/**
 * Number of outstanding log entries to keep in the log after a snapshot has
 * been taken. This avoids sending snapshots when a follower is behind by just a
 * few entries. The default is 128.
 */
RAFT_API void raft_set_snapshot_trailing(struct raft *r, unsigned n);

/**
 * Set the maximum number of a catch-up rounds to try when replicating entries
 * to a stand-by server that is being promoted to voter, before giving up and
 * failing the configuration change. The default is 10.
 */
RAFT_API void raft_set_max_catch_up_rounds(struct raft *r, unsigned n);

/**
 * Set the maximum duration of a catch-up round when replicating entries to a
 * stand-by server that is being promoted to voter. The default is 5 seconds.
 */
RAFT_API void raft_set_max_catch_up_round_duration(struct raft *r,
                                                   unsigned msecs);

RAFT_API void raft_set_state_change_cb(struct raft *r, raft_state_change_cb cb);
/**
 * Return a human-readable description of the last error occurred.
 */
RAFT_API const char *raft_errmsg(struct raft *r);

/**
 * Return the code of the current raft state.
 */
RAFT_API int raft_state(struct raft *r);

/**
 * Return the code of the current raft role.
 */
RAFT_API int raft_role(struct raft *r);

/**
 * Return the code of the current raft_io state.
 */
RAFT_API int raft_io_state(struct raft_io *io);

/**
 * Return the ID and address of the current known leader, if any.
 */
RAFT_API void raft_leader(struct raft *r, raft_id *id);

/**
 * Return the index of the last entry that was appended to the local log.
 */
RAFT_API raft_index raft_last_index(struct raft *r);

/**
 * Return the index of the last entry that was applied to the local FSM.
 */
RAFT_API raft_index raft_last_applied(struct raft *r);

/**
 * Return the index of the last entry that was commited.
 */
RAFT_API raft_index raft_commit_index(struct raft *r);

/* Common fields across client request types. */
#define RAFT__REQUEST \
    void *data;       \
    raft_time time;   \
    int type;         \
    raft_index index; \
    void *queue[2]

/**
 * Asynchronous request to append a new command entry to the log and apply it to
 * the FSM when a quorum is reached.
 */
struct raft_apply;
typedef void (*raft_apply_cb)(struct raft_apply *req, int status, void *result);
struct raft_apply
{
    RAFT__REQUEST;
    raft_apply_cb cb;
};

/**
 * Propose to append commands to the log and apply them to the FSM once
 * committed.
 *
 * If this server is the leader, it will create @n new log entries of type
 * #RAFT_COMMAND using the given buffers as their payloads, append them to its
 * own log and attempt to replicate them on other servers by sending
 * AppendEntries RPCs.
 *
 * The memory pointed at by the @base attribute of each #raft_buffer in the
 * given array must have been allocated with raft_malloc() or a compatible
 * allocator. If this function returns 0, the ownership of this memory is
 * implicitly transferred to the raft library, which will take care of releasing
 * it when appropriate. Any further client access to such memory leads to
 * undefined behavior.
 *
 * The ownership of the memory of the @bufs array itself is not transferred to
 * the raft library, and, if allocated dynamically, must be deallocated by the
 * caller.
 */
RAFT_API int raft_apply(struct raft *r,
                        struct raft_apply *req,
                        const struct raft_buffer bufs[],
                        const unsigned n,
                        raft_apply_cb cb);

/**
 * Asynchronous request to append a barrier entry.
 */
struct raft_barrier;
typedef void (*raft_barrier_cb)(struct raft_barrier *req, int status);
struct raft_barrier
{
    RAFT__REQUEST;
    raft_barrier_cb cb;
};

/**
 * Propose to append a log entry of type #RAFT_BARRIER.
 *
 * This can be used to ensure that there are no unapplied commands.
 */
RAFT_API int raft_barrier(struct raft *r,
                          struct raft_barrier *req,
                          raft_barrier_cb cb);

/**
 * Asynchronous request to change the raft configuration.
 */
typedef void (*raft_change_cb)(struct raft_change *req, int status);
struct raft_change
{
    void *data;
    raft_change_cb cb;
    raft_index index;
    raft_id match_id;
    bool cb_on_match;
};

/**
 * Add a new server to the cluster configuration. Its initial role will be
 * #RAFT_SPARE.
 */
RAFT_API int raft_add(struct raft *r,
                      struct raft_change *req,
                      raft_id id,
                      raft_change_cb cb);

/**
 * Promote server @id as voter and remove another one.
 */
RAFT_API int raft_joint_promote(struct raft *r,
                                struct raft_change *req,
                                raft_id id,
                                int role,
				                raft_id remove,
                                raft_change_cb cb);

/**
 * Assign a new role to the given server.
 *
 * If the server has already the given role, or if the given role is unknown,
 * #RAFT_BADROLE is returned.
 */
RAFT_API int raft_assign(struct raft *r,
                         struct raft_change *req,
                         raft_id id,
                         int role,
                         raft_change_cb cb);

/**
 * Remove the given server from the cluster configuration.
 */
RAFT_API int raft_remove(struct raft *r,
                         struct raft_change *req,
                         raft_id id,
                         raft_change_cb cb);
/**
 * Dup current cluster configuration.
 */
RAFT_API int raft_dup(struct raft *r,
		      struct raft_change *req,
		      raft_change_cb cb);

/**
 * Asynchronous request to transfer leadership.
 */
typedef void (*raft_transfer_cb)(struct raft_transfer *req);
struct raft_transfer
{
    void *data;               /* User data */
    raft_id id;               /* ID of target server. */
    raft_time start;          /* Start of leadership transfer. */
    struct raft_io_send send; /* For sending TimeoutNow */
    raft_transfer_cb cb;      /* User callback */
};

/**
 * Transfer leadership to the server with the given ID.
 *
 * If the target server is not part of the configuration, or it's the leader
 * itself, or it's not a #RAFT_VOTER, then #RAFT_BADID is returned.
 *
 * The special value #0 means to automatically select a voting follower to
 * transfer leadership to. If there are no voting followers, return
 * #RAFT_NOTFOUND.
 *
 * When this server detects that the target server has become the leader, or
 * when @election_timeout milliseconds have elapsed, the given callback will be
 * invoked.
 *
 * After the callback files, clients can check whether the operation was
 * successful or not by calling @raft_leader() and checking if it returns the
 * target server.
 */
RAFT_API int raft_transfer(struct raft *r,
                           struct raft_transfer *req,
                           raft_id id,
                           raft_transfer_cb cb);

/**
 * User-definable dynamic memory allocation functions.
 *
 * The @data field will be passed as first argument to all functions.
 */
struct raft_heap
{
    void *data; /* User data */
    void *(*malloc)(void *data, size_t size);
    void (*free)(void *data, void *ptr);
    void *(*calloc)(void *data, size_t nmemb, size_t size);
    void *(*realloc)(void *data, void *ptr, size_t size);
    void *(*aligned_alloc)(void *data, size_t alignment, size_t size);
    void (*aligned_free)(void *data, size_t alignment, void *ptr);
    void *(*entry_malloc)(void *data, size_t size);
    void (*entry_free)(void *data, void *ptr);
};

RAFT_API void *raft_malloc(size_t size);
RAFT_API void raft_free(void *ptr);
RAFT_API void *raft_calloc(size_t nmemb, size_t size);
RAFT_API void *raft_realloc(void *ptr, size_t size);
RAFT_API void *raft_aligned_alloc(size_t alignment, size_t size);
RAFT_API void raft_aligned_free(size_t alignment, void *ptr);
RAFT_API void *raft_entry_malloc(size_t size);
RAFT_API void raft_entry_free(void *ptr);

/**
 * Use a custom dynamic memory allocator.
 */
RAFT_API void raft_heap_set(struct raft_heap *heap);

/**
 * Use the default dynamic memory allocator (from the stdlib). This clears any
 * custom allocator specified with @raft_heap_set.
 */
RAFT_API void raft_heap_set_default(void);

/**
 * User-definable configuration encode/decode functions.
 *
 * The @data field will be passed as first argument to all functions.
 */
struct raft_configuration_codec
{
	void *data; /* User data */
	int (*encode)(void *ptr,
		      const struct raft_configuration *c,
		      struct raft_buffer *buf);
	int (*decode)(void *ptr,
		      const struct raft_buffer *buf,
		      struct raft_configuration *c);
};

/**
 * Use a custom configuration codec.
 */
RAFT_API void raft_configuration_codec_set(
					struct raft_configuration_codec *codec);

/**
 * Use the default configuration codec. This clears any
 * custom codec specified with @raft_configuration_codec_set.
 */
RAFT_API void raft_configuration_codec_set_default(void);

/**
 *  Set log threshold per message. default(32)
 */
RAFT_API void raft_set_replication_message_log_threshold(struct raft *r,
							 unsigned n);
/**
 * Set replication inflight log threshold. default(1024)
 */
RAFT_API void raft_set_replication_inflight_log_threshold(struct raft *r,
							  unsigned n);
/**
 * set custom tracer
 * @param r
 * @param tracer
 */
RAFT_API void raft_set_tracer(struct raft *r, struct raft_tracer *tracer);

/**
 * User-definable hook for predefined points
 */
struct raft_hook
{
	void *data;
	void (*entry_after_append_fn)(struct raft_hook *h, raft_index index,
				      const struct raft_entry *entry);
	void (*entry_match_change_cb)(struct raft_hook *h, bool match,
				      raft_index index, raft_term term);
	void (*entry_after_apply_fn)(struct raft_hook *h, raft_index index,
				     const struct raft_entry *entry);
    /**
     * Check skip retry after apply failed.
     */
	bool (*entry_skip_on_apply_fail)(struct raft_hook *h, raft_index index,
					 const struct raft_entry *entry);
	/**
     * Check entry at @index should apply now.
     * @return false entry couldn't apply, try later.
     */
	bool (*entry_should_apply)(struct raft_hook *h, raft_index index,
				   const struct raft_entry *entry);
	void (*request_accept)(struct raft_hook *h, struct request *req);
	void (*request_append)(struct raft_hook *h, struct request *req);
	void (*request_append_done)(struct raft_hook *h, struct request *req);
	void (*request_match)(struct raft_hook *h, struct request *req,
			      raft_id id);
	void (*request_commit)(struct raft_hook *h, struct request *req);
	void (*request_apply)(struct raft_hook *h, struct request *req);
	void (*request_apply_done)(struct raft_hook *h, struct request *req);
	raft_index (*get_next_snapshot_index)(struct raft_hook *h);
    void (*conf_change)(struct raft_hook *h, const struct raft_configuration *c);
    bool (*hack_append_entries)(struct raft_hook *h,
                                const struct raft_append_entries *ae,
                                struct raft_append_entries_result *result,
                                bool *discard);
};

RAFT_API void raft_set_hook(struct raft *r, struct raft_hook * hook);

RAFT_API bool raft_aux_match_leader(struct raft *r);


/* event level */
enum raft_event_level {
	RAFT_ERROR = 0,
	RAFT_WARN,
	RAFT_NOTICE,
	RAFT_INFO,
	RAFT_DEBUG,
};

/* User-definable event recorder for record events */
struct raft_event_recorder {
	void *data;
	bool (*is_id_allowed)(void *data, raft_id id);
	void (*record)(void *data, enum raft_event_level level, const char *fn,
		       const char *file, int line, const char *fmt, ...);
    int  (*get_level)(void *data);
};

/* set user defined event recoder */
RAFT_API void raft_set_event_recorder(struct raft_event_recorder *r);

/**
 * Set whether leader should sync all replica before take snapshot
 */
RAFT_API void raft_set_sync_replication(struct raft *r, bool sync);

/* Set quorum for raft */
RAFT_API void raft_set_quorum(struct raft *r, enum raft_quorum q);

/**
 * Replace current configuration. This api must be called when raft's state is
 * follower
 */
RAFT_API int raft_replace_configuration(struct raft *r,
				        struct raft_configuration conf);
/**
 * Set whether follower should take snapshot sync with leader
 */
RAFT_API void raft_set_sync_snapshot(struct raft *r , bool sync);

/**
 * Set sync replication time out
 */
RAFT_API void raft_set_sync_replica_timeout_min(struct raft *r, unsigned msecs);

RAFT_API void raft_set_sync_replica_timeout_max(struct raft *r, unsigned msecs);

/**
 * Get min sync match index
 */
RAFT_API raft_index raft_min_sync_match_index(struct raft *r);

/**
 * Get min sync match replica id
 */
RAFT_API raft_id raft_min_sync_match_replica(struct raft *r);

/**
 * Set whether nonvoter can grant vote
 */
RAFT_API void raft_set_non_voter_grant_vote(struct raft *r, bool grant);

/**
 * Set request hook flag @enable
 */
RAFT_API void raft_enable_request_hook(struct raft *r, bool enable);

/**
 * Set enable dynamic trailing @enable
 */
RAFT_API void raft_enable_dynamic_trailing(struct raft *r, bool enable);

/**
 * Set max dynamic trailing
 */
RAFT_API void raft_set_max_dynamic_trailing(struct raft *r, unsigned trailing);

/*
* Set the only voter elect as leader at start
*/
RAFT_API void raft_enable_election_at_start(struct raft *r, bool enable);

/**
 * Check candidate is distruptive
 */
RAFT_API bool raft_is_distruptive_candidate(struct raft *r);

/**
 * Set snapshot sample span in ms.
 * Return 0 or RAFT_NOMEM
 */
RAFT_API int raft_set_snapshot_sample_span(struct raft *r, unsigned span);

/**
 * Change raft's current role
 */
RAFT_API void raft_set_role(struct raft *r, int role);

/**
 * Get the first request.
 */
RAFT_API struct request *raft_first_request(struct raft *r);

/**
 *  Set leader stepdown cb
 */
RAFT_API void raft_set_leader_stepdown_cb(struct raft *r,
                                          raft_leader_stepdown_cb cb);

/**
 * Get the latest entry time.
 */
RAFT_API raft_time raft_latest_entry_time(struct raft *r);

typedef int (raft_dump_fn)(char *fmt, ...);
/**
 * Dump raft's internal status
 */
RAFT_API void raft_dump(struct raft *r, raft_dump_fn dump);

/**
 * Set log hook.
 */
RAFT_API void raft_set_log_hook(struct raft *r, struct raft_log_hook *hook);

/**
 * Set aggressive snapshot.
 */
RAFT_API void raft_set_aggressive_snapshot(struct raft *r, bool enable,
					   unsigned threshold, unsigned trailing);

/**
 * Enable call callback of configuration change when replica's progress match
 * index.
 */
RAFT_API void raft_enable_change_cb_on_match(struct raft *r, bool enable);

#undef RAFT__REQUEST

#endif /* RAFT_H */
