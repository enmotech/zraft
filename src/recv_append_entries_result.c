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
    int rv = 0;

	bool pgrep_proc = false;

    assert(r != NULL);
    assert(id > 0);
    //assert(address != NULL);
    assert(result != NULL);

	unsigned i = configurationIndexOf(&r->configuration, id);

	(void)(address);
	
    ZSINFO(gzlog, "[raft][%d][%d][pkt:%d]recvAppendEntriesResult: peer[%d], replicating[%d] permit[%d] rejected[%lld] last_log_index[%lld]",
		   rkey(r), r->state, result->pkt, i, result->pi.replicating, result->pi.permit, result->rejected, result->last_log_index);

    if (r->state != RAFT_LEADER) {
        tracef("local server is not leader -> ignore");
        goto __pgrep_prco;
    }

    rv = recvEnsureMatchingTerms(r, result->term, &match);
    if (rv != 0) {
        goto __pgrep_prco;
    }

    if (match < 0) {
        tracef("local term is higher -> ignore ");
        goto __pgrep_prco;
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
        goto __pgrep_prco;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        tracef("unknown server -> ignore");
        goto __pgrep_prco;
    }

	if (result->pi.replicating == PGREP_RND_HRT)
		goto __pgrep_prco;

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server, result);
    if (rv != 0) {
        goto __pgrep_prco;
    }

	pgrep_proc = true;

__pgrep_prco:

	/* Pgrep:
	 *
	 * Release the pgrep permit. And check the returned status.
	 */
	if (result->pi.permit) {

		raft_index prev_applied_index = min(
			max(result->last_log_index, r->log.offset), r->last_applied);
		bool unpermit = false;

		if (!pgrep_proc) {
			unpermit = true;
			r->io->pgrep_cancel(r->io);
		} else {

			/* Catch-up meet some error. */
			if (result->pi.replicating == PGREP_RND_ERR) {
				unpermit = true;
				r->io->pgrep_cancel(r->io);

			} else  if (result->pi.replicating == PGREP_RND_BGN ||
						result->pi.replicating == PGREP_RND_ING) {

				progressUpdateAppliedIndex(r, i, prev_applied_index);

				if (r->last_applied > prev_applied_index && i == r->pgrep_id)
					replicationProgress(r, r->pgrep_id, result->pi);
				else {
					unpermit = true;
				}
			}

		}

		if (unpermit) {
			r->io->pgrep_raft_unpermit(r->io, RAFT_APD, &result->pi);
			ZSINFO(gzlog, "[raft][%d][%d]recvAppendEntriesResult: pgrep permit released.",
				   rkey(r), r->state);
		}
	}

    return rv;
}

#undef tracef
