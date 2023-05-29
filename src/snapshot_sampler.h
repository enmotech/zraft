#ifndef RAFT_SNAPSHOT_SAMPLER_H_
#define RAFT_SNAPSHOT_SAMPLER_H_
#include "../include/raft.h"

int snapshotSamplerInit(struct raft_snapshot_sampler *s, raft_time span,
			raft_time period, raft_time now);

void snapshotSamplerClose(struct raft_snapshot_sampler *s);

void snapshotSamplerTake(struct raft_snapshot_sampler *s,
			 raft_index snapshot_index, raft_time time);

raft_index snapshotSamplerFirstIndex(struct raft_snapshot_sampler *s);
#endif /* RAFT_SNAPSHOT_SAMPLER_H */