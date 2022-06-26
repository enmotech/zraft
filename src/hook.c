#include "../include/raft.h"

static void defaultAppendPostProcess(struct raft_hook *hook, raft_index index,
				     const struct raft_entry *entry)
{
	(void)hook;
	(void)index;
	(void)entry;
}

struct raft_hook defaultHook = {
	.data = NULL,
	.append_post_process = defaultAppendPostProcess,
};
