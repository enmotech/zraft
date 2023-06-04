#ifndef HOOK_H
#define HOOK_H
#include "../include/raft.h"

extern struct raft_hook defaultHook;

void hookRequestAccept(struct raft *r, raft_index index);
void hookRequestAppend(struct raft *r, raft_index index);
void hookRequestAppendDone(struct raft *r, raft_index index);
void hookRequestMatch(struct raft *r, raft_index index, size_t n, raft_id id);
void hookRequestCommit(struct raft *r, raft_index index, size_t n);
void hookRequestApply(struct raft *r, raft_index index);
void hookRequestApplyDone(struct raft *r, raft_index index);

void hookConfChange(struct raft *r, const struct raft_configuration *c);

bool hookHackAppendEntries(struct raft *r,
                           const struct raft_append_entries *ae,
                           struct raft_append_entries_result *result);

#endif //HOOK_H
