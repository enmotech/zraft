/* InstallSnapshot RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include "../include/raft.h"

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
                        raft_id id,
                        struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
