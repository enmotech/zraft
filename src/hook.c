#include "../include/raft.h"
#include "request.h"
#include "assert.h"

#define HOOK_MAX_BATCH_SIZE 128
#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif



static void defaultEntryAfterAppend(struct raft_hook *h, raft_index index,
				    const struct raft_entry *entry)
{
	(void)h;
	(void)index;
	(void)entry;
}

static void defaultEntryMatchChange(struct raft_hook *h, bool match,
				    raft_index index, raft_term term)
{
	(void)h;
	(void)match;
	(void)index;
	(void)term;
}

static void defaultEntryAfterApply(struct raft_hook *h, raft_index index,
				   const struct raft_entry *entry)
{
	(void)h;
	(void)index;
	(void)entry;
}

static void defaultRequestDummy(struct raft_hook *h, struct request *req)
{
	(void)h;
	(void)req;
}

static void defaultRequestMatch(struct raft_hook *h, struct request *req,
				raft_id id)
{
	(void)h;
	(void)req;
	(void)id;
}

static void defaultConfChange(struct raft_hook *h,
			      const struct raft_configuration *c)
{
	(void)h;
	(void)c;
}

static bool defaultHackAppendEntries(struct raft_hook *h,
				     const struct raft_append_entries *ae,
				     struct raft_append_entries_result *result)
{
	(void)h;
	(void)ae;
	(void)result;

	return false;
}

struct raft_hook defaultHook = {
	.data = NULL,
	.entry_after_append_fn = defaultEntryAfterAppend,
	.entry_match_change_cb = defaultEntryMatchChange,
	.entry_after_apply_fn  = defaultEntryAfterApply,
	.conf_change = defaultConfChange,
	.request_accept =  defaultRequestDummy,
	.request_append = defaultRequestDummy,
	.request_append_done = defaultRequestDummy,
	.request_match = defaultRequestMatch,
	.request_commit = defaultRequestDummy,
	.request_apply = defaultRequestDummy,
	.request_apply_done = defaultRequestDummy,
	.hack_append_entries = defaultHackAppendEntries,
};

void hookRequestAccept(struct raft *r, raft_index index)
{
	struct request *req;
	assert(r->state == RAFT_LEADER);
	if (!r->enable_request_hook)
		return;
	req = requestRegFind(&r->leader_state.reg, index);
	if (req == NULL)
		return;
	r->hook->request_accept(r->hook, req);
}

void hookRequestAppend(struct raft *r, raft_index index)
{
	struct request *req;
	assert(r->state == RAFT_LEADER);
	if (!r->enable_request_hook)
		return;
	req = requestRegFind(&r->leader_state.reg, index);
	if (req == NULL)
		return;
	r->hook->request_append(r->hook, req);
}

void hookRequestAppendDone(struct raft *r, raft_index index)
{
	struct request *req;
	if (!r->enable_request_hook)
		return;
	if (r->state != RAFT_LEADER)
		return;
	req = requestRegFind(&r->leader_state.reg, index);
	if (req == NULL)
		return;
	r->hook->request_append_done(r->hook, req);
}

void hookRequestMatch(struct raft *r, raft_index index, size_t n, raft_id id)
{
	size_t i;
	struct request *req;

	assert(r->state == RAFT_LEADER);
	if (!r->enable_request_hook)
		return;
	if (index + n - 1 <= r->last_applying)
		return;
	n = min(n, HOOK_MAX_BATCH_SIZE);
	for (i = 0; i < n; ++i) {
		req = requestRegFind(&r->leader_state.reg, index + i);
		if (req == NULL)
			continue;
		r->hook->request_match(r->hook, req, id);
	}
}

void hookRequestCommit(struct raft *r, raft_index index, size_t n)
{
	size_t i;
	struct request *req;

	assert(r->state == RAFT_LEADER);
	if (!r->enable_request_hook)
		return;
	n = min(n, HOOK_MAX_BATCH_SIZE);
	for (i = 0; i < n; ++i) {
		req = requestRegFind(&r->leader_state.reg, index + i);
		if (req == NULL)
			continue;
		r->hook->request_commit(r->hook, req);
	}
}

void hookRequestApply(struct raft *r, raft_index index)
{
	struct request *req;
	if (!r->enable_request_hook)
		return;
	if (r->state != RAFT_LEADER)
		return;
	req = requestRegFind(&r->leader_state.reg, index);
	if (req == NULL)
		return;
	r->hook->request_apply(r->hook, req);
}

void hookRequestApplyDone(struct raft *r, raft_index index)
{
	struct request *req;
	if (!r->enable_request_hook)
		return;
	if (r->state != RAFT_LEADER)
		return;
	req = requestRegFind(&r->leader_state.reg, index);
	if (req == NULL)
		return;
	r->hook->request_apply_done(r->hook, req);
}

void hookConfChange(struct raft *r, const struct raft_configuration *c)
{
	if (!r->hook->conf_change)
		return;
	r->hook->conf_change(r->hook, c);
}

bool hookHackAppendEntries(struct raft *r,
                           const struct raft_append_entries *ae,
			   struct raft_append_entries_result *result)
{
	if (!r->hook->hack_append_entries)
		return false;
	return r->hook->hack_append_entries(r->hook, ae, result);
}