#ifndef JOINT_CONSENSUS_H
#define JOINT_CONSENSUS_H

#include "../include/raft.h"

// void jointReconfCb(struct raft_change *req, int status);

/* 调用该接口将新配置存储Cnew，并把需要添加的节点添加到现有的server数组 */
int jointDecodeNewConf(struct raft *r, struct raft_configuration *configuration,
    struct raft_server *servers, unsigned int n);

/* 该接口判断joint阶段的大多数 */
void jointReplicationQuorum(struct raft *r, const raft_index index);

bool jointElectionTally(struct raft *r);
int jointChangePhase(struct raft *r, struct raft_configuration *configuration, enum configuration_phase new_phase);
unsigned int jointNQuorum(struct raft *r);
bool jointServerIsInCnew(struct raft *r, const struct raft_configuration *c,
                                           const raft_id id);
unsigned jointConfigurationVoterCount(const struct raft_configuration *c);
#endif