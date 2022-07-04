#include "../include/raft.h"

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

struct raft_hook defaultHook = {
	.data = NULL,
	.entry_after_append_fn = defaultEntryAfterAppend,
	.entry_match_change_cb = defaultEntryMatchChange,
};
