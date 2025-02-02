/* Logic to be invoked periodically. */

#ifndef TICK_H_
#define TICK_H_

#include "../include/raft.h"

/* Callback to be passed to the @raft_io implementation. It notifies us that a
 * certain amount of time has elapsed and will be invoked periodically. */
void tickCb(struct raft_io *io);

/**
 * Check whether leader is in contact with the majority.
 */
bool tickCheckContactQuorum(struct raft *r);

#endif /* TICK_H_ */
