/* Receive an RPC message. */

#ifndef RECV_H_
#define RECV_H_

#include "../include/raft.h"

/* Callback to be passed to the raft_io implementation. It will be invoked upon
 * receiving an RPC message. */
void recvCb(struct raft_io *io, struct raft_message *message);

/* Compare a request's term with the server's current term.
 *
 * The match output parameter will be set to 0 if the local term matches the
 * request's term, to -1 if the request's term is lower, and to 1 if the
 * request's term is higher. */
void recvCheckMatchingTerms(struct raft *r, raft_term term, int *match);

/* Common logic for RPC handlers, comparing the request's term with the server's
 * current term and possibly deciding to reject the request or step down from
 * candidate or leader.
 *
 * From Section 3.3:
 *
 *   If a candidate or leader discovers that its term is out of date, it
 *   immediately reverts to follower state. If a server receives a request with
 *   a stale term number, it rejects the request.
 *
 * The match output parameter will be set to 0 if the local term matches the
 * request's term, to -1 if the request's term is lower, and to 1 if the
 * request's term was higher but we have successfully bumped the local one to
 * match it (and stepped down to follower in that case, if we were not
 * follower already). */

/* If different from the current one, update information about the current
 * leader. Must be called only by followers. */
int recvUpdateLeader(struct raft *r, raft_id id);

int recvUpdateMeta(struct raft *r, struct raft_message *message, raft_term term,
                   raft_id voted_for, raft_io_set_meta_cb cb);
#endif /* RECV_H_ */
