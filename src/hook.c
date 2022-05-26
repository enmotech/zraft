#include "../include/raft.h"

static void defaultConfAfterAppend(struct raft_hook *hook, raft_index index,
				   const struct raft_entry *entry)
{
	(void)hook;
	(void)index;
	(void)entry;
}

struct raft_hook defaultHook = {
	.data = NULL,
	.conf_after_append = defaultConfAfterAppend,
};
