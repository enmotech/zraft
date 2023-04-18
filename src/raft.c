#include "../include/raft.h"

#include <string.h>

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "err.h"
#include "heap.h"
#include "log.h"
#include "membership.h"
#include "tracing.h"
#include "hook.h"
#include "event.h"

#define DEFAULT_ELECTION_TIMEOUT 1000 /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100 /* One tenth of a second */
#define DEFAULT_INSTALL_SNAPSHOT_TIMEOUT 30000 /* 30 seconds */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048
#define DEFAULT_MESSAGE_LOG_THRESHOLD 32
#define DEFAULT_INFLIGHT_LOG_THRESHOLD 0
#define DEFAULT_SYNC_REPLICATION_TIMEOUT 3000

/* Number of milliseconds after which a server promotion will be aborted if the
 * server hasn't caught up with the logs yet. */
#define DEFAULT_MAX_CATCH_UP_ROUNDS 10
#define DEFAULT_MAX_CATCH_UP_ROUND_DURATION (5 * 1000)

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              const raft_id id)
{
    int rv;
    assert(r != NULL);
    r->io = io;
    r->io->data = r;
    r->fsm = fsm;
    r->tracer = &NoopTracer;
    r->id = id;
    r->current_term = 0;
    r->voted_for = 0;
    logInit(&r->log);
    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->reset_trailing_timeout = 30 * DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->install_snapshot_timeout = DEFAULT_INSTALL_SNAPSHOT_TIMEOUT;
    r->commit_index = 0;
    r->last_applying = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->transfer = NULL;
    r->snapshot.pending.term = 0;
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    raft_configuration_init(&r->snapshot.configuration);
    r->snapshot.put.data = NULL;
    r->close_cb = NULL;
    memset(r->errmsg, 0, sizeof r->errmsg);
    r->pre_vote = false;
    r->max_catch_up_rounds = DEFAULT_MAX_CATCH_UP_ROUNDS;
    r->max_catch_up_round_duration = DEFAULT_MAX_CATCH_UP_ROUND_DURATION;
    r->message_log_threshold = DEFAULT_MESSAGE_LOG_THRESHOLD;
    r->inflight_log_threshold = DEFAULT_INFLIGHT_LOG_THRESHOLD;
    r->hook = &defaultHook;
    r->sync_replication = false;
    r->sync_snapshot = false;
    r->sync_replication_timeout = DEFAULT_SYNC_REPLICATION_TIMEOUT;
    r->nr_appending_requests = 0;
    r->prev_append_status = 0;
    r->quorum = RAFT_MAJORITY;
    r->non_voter_grant_vote = false;
    r->enable_request_hook = false;
    r->enable_dynamic_trailing = false;
    r->enable_free_trailing = false;
    r->enable_election_at_start = true;
    rv = r->io->init(r->io, r->id);
    r->state_change_cb = NULL;
    if (rv != 0) {
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        evtErrf("raft(%llx) init failed %d", r->id, rv);
        goto err_after_address_alloc;
    }
    return 0;

err_after_address_alloc:
    assert(rv != 0);
    return rv;
}

int raft_io_state(struct raft_io *io)
{
	return io->state;
}

static void ioCloseCb(struct raft_io *io)
{
    struct raft *r = io->data;
    logClose(&r->log);
    raft_configuration_close(&r->configuration);
    raft_configuration_close(&r->snapshot.configuration);
    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}

void raft_close(struct raft *r, bool clean, void (*cb)(struct raft *r))
{
    assert(r->close_cb == NULL);
    if (r->state != RAFT_UNAVAILABLE) {
        convertToUnavailable(r);
    }
    r->close_cb = cb;
    r->io->close(r->io, clean, ioCloseCb);
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

void raft_set_install_snapshot_timeout(struct raft *r, const unsigned msecs)
{
    r->install_snapshot_timeout = msecs;
}

void raft_set_snapshot_threshold(struct raft *r, unsigned n)
{
    r->snapshot.threshold = n;
}

void raft_set_snapshot_trailing(struct raft *r, unsigned n)
{
    r->snapshot.trailing = n;
}

void raft_set_max_catch_up_rounds(struct raft *r, unsigned n)
{
    r->max_catch_up_rounds = n;
}

void raft_set_max_catch_up_round_duration(struct raft *r, unsigned msecs)
{
    r->max_catch_up_round_duration = msecs;
}

void raft_set_pre_vote(struct raft *r, bool enabled)
{
    r->pre_vote = enabled;
}

const char *raft_errmsg(struct raft *r)
{
    return r->errmsg;
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        evtErrf("raft(%llx) raft state %d", r->id, r->state);
        return RAFT_BUSY;
    }

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        evtErrf("raft(%llx) bootstrap failed %d", r->id, rv);
        return rv;
    }

    return 0;
}
int raft_abootstrap(struct raft *r,
           struct raft_io_bootstrap *req,
           const struct raft_configuration *conf,
           raft_io_bootstrap_cb cb)
{
    int rv;
    raft_id id = r->id;

    if (r->state != RAFT_UNAVAILABLE) {
        evtErrf("raft(%llx) raft state %d", r->id, r->state);
        return RAFT_BUSY;
    }

    rv = r->io->abootstrap(r->io, req, conf, cb);
    if (rv != 0) {
        evtErrf("raft(%llx) abootstrap failed %d", id, rv);
        return rv;
    }

    return 0;
}

void raft_set_state_change_cb(struct raft *r, raft_state_change_cb cb)
{
	r->state_change_cb = cb;
}

int raft_recover(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        evtErrf("raft(%llx) state is ", r->id, r->state);
        return RAFT_BUSY;
    }

    rv = r->io->recover(r->io, conf);
    if (rv != 0) {
        evtErrf("raft(%llx) recover failed %d", r->id, rv);
        return rv;
    }

    return 0;
}

const char *raft_strerror(int errnum)
{
    return errCodeToString(errnum);
}

void raft_configuration_init(struct raft_configuration *c)
{
    configurationInit(c);
}

void raft_configuration_close(struct raft_configuration *c)
{
    configurationClose(c);
}

int raft_configuration_add(struct raft_configuration *c,
                           const raft_id id,
                           const int role)
{
    assert(c->phase == RAFT_CONF_NORMAL);
    return configurationAdd(c, id, role, role, RAFT_GROUP_OLD);
}

int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    return configurationEncode(c, buf);
}

unsigned raft_configuration_voter_count(const struct raft_configuration *c)
{
    return configurationVoterCount(c, RAFT_GROUP_ANY);
}

unsigned long long raft_digest(const char *text, unsigned long long n)
{
    struct byteSha1 sha1;
    uint8_t value[20];
    uint64_t n64 = byteFlip64((uint64_t)n);
    uint64_t digest;

    byteSha1Init(&sha1);
    byteSha1Update(&sha1, (const uint8_t *)text, (uint32_t)strlen(text));
    byteSha1Update(&sha1, (const uint8_t *)&n64, (uint32_t)(sizeof n64));
    byteSha1Digest(&sha1, value);

    memcpy(&digest, value + (sizeof value - sizeof digest), sizeof digest);

    return byteFlip64(digest);
}

void raft_set_replication_message_log_threshold(struct raft *r, unsigned n)
{
	r->message_log_threshold = n;
}

void raft_set_replication_inflight_log_threshold(struct raft *r, unsigned n)
{
	r->inflight_log_threshold = n;
}

void raft_set_hook(struct raft *r, struct raft_hook *hook)
{
	r->hook = hook;
}

void raft_set_tracer(struct raft *r, struct raft_tracer *tracer)
{
	r->tracer = tracer;
}

bool raft_aux_match_leader(struct raft *r)
{
	assert(r->state == RAFT_FOLLOWER);

	return r->follower_aux.match_leader;
}

void raft_set_sync_replication(struct raft *r, bool sync)
{
	r->sync_replication = sync;
}

void raft_set_quorum(struct raft *r, enum raft_quorum q)
{
	assert(q == RAFT_MAJORITY || q == RAFT_FULL);
	r->quorum = q;
}

RAFT_API int raft_replace_configuration(struct raft *r,
					struct raft_configuration conf)
{
	if (r->io->state != RAFT_IO_AVAILABLE) {
		evtNoticef("raft(%llx) io busy %u", r->id, r->io->state);
		return RAFT_BUSY;
	}
	if (r->state != RAFT_FOLLOWER)
		convertToFollower(r);
	assert(r->state == RAFT_FOLLOWER);
	raft_configuration_close(&r->configuration);
	r->configuration = conf;

	evtNoticef("raft(%llx) conf replace", r->id);
	evtDumpConfiguration(r, &conf);
    hookConfChange(r, &conf);
	return 0;
}

void raft_set_sync_snapshot(struct raft *r , bool sync)
{
	r->sync_snapshot = sync;
}

void raft_set_sync_replication_timeout(struct raft *r, unsigned msecs)
{
	r->sync_replication_timeout = msecs;
}

void raft_set_non_voter_grant_vote(struct raft *r, bool grant)
{
	r->non_voter_grant_vote = grant;
}

void raft_enable_request_hook(struct raft *r, bool enable)
{
	r->enable_request_hook = enable;
}

void raft_enable_dynamic_trailing(struct raft *r, bool enable){
    (void)r;
    (void)enable;
}

void raft_enable_free_trailing(struct raft *r, bool enable){
    (void)r;
    (void)enable;
}

void raft_enable_election_at_start(struct raft *r, bool enable)
{
    r->enable_election_at_start = enable;
}

bool raft_is_distruptive_candidate(struct raft *r)
{
    assert(r->state == RAFT_CANDIDATE);

    return r->candidate_state.disrupt_leader;
}