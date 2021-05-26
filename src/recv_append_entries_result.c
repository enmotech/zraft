#include "recv_append_entries_result.h"
#include "assert.h"
#include "configuration.h"
#include "tracing.h"
#include "recv.h"
#include "replication.h"
#include "progress.h"


/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

int recvAppendEntriesResult(struct raft *r,
                            const raft_id id,
                            const char *address,
                            const struct raft_append_entries_result *result)
{
    int match;
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    //assert(address != NULL);
    assert(result != NULL);

	unsigned i = configurationIndexOf(&r->configuration, id);

	(void)(address);
	
    ZSINFO(gzlog, "[raft][%d][%d][pkt:%d]recvAppendEntriesResult: peer[%d], replicating[%d] permit[%d] rejected[%lld] last_log_index[%lld]",
		   rkey(r), r->state, result->pkt, i, result->pi.replicating, result->pi.permit, result->rejected, result->last_log_index);

	/* Pgrep:
     *
	 * Release the pgrep permit. And check the returned status.
     */
	if (result->pi.permit) {
		r->io->pgrep_raft_unpermit(r->io, RAFT_APD, &result->pi);

		ZSINFO(gzlog, "[raft][%d][%d]recvAppendEntriesResult: pgrep permit released.",
				   rkey(r), r->state);

		/* Catch-up say replicating can't going on, so set it spare. */
		if (result->pi.replicating == PGREP_RND_BRK) {
			/* TODO:  */
			r->io->pgrep_cancel(r->io);
		}

		/* Catch-up meet some error. */
		if (result->pi.replicating == PGREP_RND_ERR) {
			r->io->pgrep_cancel(r->io);
		}
	}

    if (r->state != RAFT_LEADER) {
        tracef("local server is not leader -> ignore");
        return 0;
    }

    rv = recvEnsureMatchingTerms(r, result->term, &match);
    if (rv != 0) {
        return rv;
    }

    if (match < 0) {
        tracef("local term is higher -> ignore ");
        return 0;
    }

    /* If we have stepped down, abort here.
     *
     * From Figure 3.1:
     *
     *   [Rules for Servers] All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     */
    if (match > 0) {
        assert(r->state == RAFT_FOLLOWER);
        return 0;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        tracef("unknown server -> ignore");
        return 0;
    }

	/* Update pgrep prev_applied_index to cur_applied_index. */
	if (result->pi.permit && result->pi.replicating == PGREP_RND_ING)
		progressUpdateAppliedIndex(r, i, r->last_applied);

	if (result->pi.replicating == PGREP_RND_HRT)
		return 0;

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server, result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef tracef
