#include "../lib/munit_mock.h"
#include "../../src/log.h"
#include "../../src/request.h"
#include "../../src/replication.h"
#include "../../src/configuration.h"

extern int __real_logAppendCommands(struct raft_log *l,
                                    const raft_term term,
                                    const struct raft_buffer bufs[],
                                    const unsigned n);

int __wrap_logAppendCommands(struct raft_log *l,
                             const raft_term term,
                             const struct raft_buffer bufs[],
                             const unsigned n)
{
    return mock_type_args(int, logAppendCommands, l, term, bufs, n);
}

extern int __real_requestRegEnqueue(struct request_registry *reg,
                                    struct request *req);

int __wrap_requestRegEnqueue(struct request_registry *reg, struct request *req)
{
    return mock_type_args(int, requestRegEnqueue, reg, req);
}

extern int __real_replicationTrigger(struct raft *r, raft_index index);

int __wrap_replicationTrigger(struct raft *r, raft_index index)
{
    return mock_type_args(int, replicationTrigger, r, index);
}

extern int __real_logAppend(struct raft_log *l,
                            const raft_term term,
                            const unsigned short type,
                            const struct raft_buffer *buf,
                            void *batch);

int __wrap_logAppend(struct raft_log *l,
                     const raft_term term,
                     const unsigned short type,
                     const struct raft_buffer *buf,
                     void *batch)
{
    return mock_type_args(int, logAppend, l, term, type, buf, batch);
}


extern int __real_configurationCopy(const struct raft_configuration *src,
                                    struct raft_configuration *dst);


int __wrap_configurationCopy(const struct raft_configuration *src,
                             struct raft_configuration *dst)
{
    return mock_type_args(int, configurationCopy, src, dst);
}

extern int __real_logAcquire(struct raft_log *l, const raft_index index,
                             struct raft_entry *entries[], unsigned *n,
                             unsigned max);

int __wrap_logAcquire(struct raft_log *l, const raft_index index,
                      struct raft_entry *entries[], unsigned *n, unsigned max)
{
    return mock_type_args(int, logAcquire, l, index, entries, n, max);
}