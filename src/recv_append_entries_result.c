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
    assert(address != NULL);
    assert(result != NULL);
	
    ZSINFO(gzlog, "[raft][%d]recvAppendEntriesResult: replicating[%d] permit[%d]",
           r->state, result->pi.replicating, result->pi.permit);

	/* Pgrep:
     *
	 * Release the pgrep permit. If the returned last_log_index not equals to r->last_applied,
	 * means Catch-up failed applied the log entries, so here cancel pgrep to cause a pgrep failure.
     */
	if (result->pi.permit) {
		r->io->pgrep_raft_unpermit(r->io, RAFT_APD, &result->pi);

		if (result->pi.replicating == PGREP_RND_ING &&
			result->last_log_index != r->last_applied) {
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
	if (result->pi.permit) {
		unsigned i = configurationIndexOf(&r->configuration, id);
		progressUpdateAppliedIndex(r, i, r->last_applied);
	}

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server, result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef tracef
