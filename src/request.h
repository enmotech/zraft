#ifndef REQUEST_H_
#define REQUEST_H_

#include "../include/raft.h"


/* Init an empty request registry */
void requestRegInit(struct request_registry *reg);

/* Release memory used by registry */
void requestRegClose(struct request_registry *reg);

/* Add request to registry */
int requestRegEnqueue(struct request_registry *reg, struct request *req);

/* Delete request by index */
struct request *requestRegDel(struct request_registry *reg, raft_index index);

/* Find request by index */
struct request *requestRegFind(struct request_registry *reg, raft_index index);

/* Dequeue the first request */
struct request *requestRegDequeue(struct request_registry *reg);

/* Number of requests */
size_t requestRegNumRequests(struct request_registry *reg);

#endif /* REQUEST_H_ */
