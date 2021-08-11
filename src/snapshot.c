#include "snapshot.h"

#include <stdint.h>
#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "err.h"
#include "log.h"
#include "tracing.h"

void snapshotClose(struct raft_snapshot *s)
{
    unsigned i;
    configurationClose(&s->configuration);
    for (i = 0; i < s->n_bufs; i++) {
        raft_free(s->bufs[i].base);
    }
    raft_free(s->bufs);
}

void snapshotDestroy(struct raft_snapshot *s)
{
    snapshotClose(s);
    raft_free(s);
}

int snapshotRestore(struct raft *r, struct raft_snapshot *snapshot)
{
    int rv;

    rv = r->fsm->restore(r->fsm, &snapshot->bufs[0]);
    if (rv != 0) {
        tracef("restore snapshot %llu: %s", snapshot->index,
               errCodeToString(rv));
        return rv;
    }
    configurationClose(&r->snapshot.configuration);
    rv = configurationCopy(&snapshot->configuration,
                           &r->snapshot.configuration);
    if (rv != 0) {
        tracef("restore snapshot %llu: %s", snapshot->index,
               errCodeToString(rv));
        return rv;
    }
    configurationClose(&r->configuration);
    r->configuration = snapshot->configuration;
    r->configuration_index = snapshot->configuration_index;

    r->commit_index = snapshot->index;
    r->last_applied = snapshot->index;
    r->last_applying = snapshot->index;
    r->last_stored = snapshot->index;

    /* Don't free the snapshot data buffer, as ownership has been transferred to
     * the fsm. */
    raft_free(snapshot->bufs);

    tracef("[raft][%d][%d][%s][conf_dump]", rkey(r), r->state, __func__);
    for (unsigned int i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
		(void)(server);
        tracef("[raft][%d][%d][%s][conf_dump] i[%d] id[%lld] role[%d] pre_role[%d]",
               rkey(r), r->state, __func__, i,
               server->id, server->role, server->pre_role);
    }

    return 0;
}

int snapshotCopy(const struct raft_snapshot *src, struct raft_snapshot *dst)
{
    int rv;
    unsigned i;
    size_t size;
    uint8_t *cursor;

    dst->term = src->term;
    dst->index = src->index;

    rv = configurationCopy(&src->configuration, &dst->configuration);
    if (rv != 0) {
        return rv;
    }

    size = 0;
    for (i = 0; i < src->n_bufs; i++) {
        size += src->bufs[i].len;
    }

    dst->bufs = raft_malloc(sizeof *dst->bufs);
    assert(dst->bufs != NULL);

    dst->bufs[0].base = raft_malloc(size);
    dst->bufs[0].len = size;
    if (dst->bufs[0].base == NULL) {
        return RAFT_NOMEM;
    }

    cursor = dst->bufs[0].base;

    for (i = 0; i < src->n_bufs; i++) {
        memcpy(cursor, src->bufs[i].base, src->bufs[i].len);
        cursor += src->bufs[i].len;
    }

    dst->n_bufs = 1;

    return 0;
}

#undef tracef
