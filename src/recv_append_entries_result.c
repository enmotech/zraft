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
	
	ZSINFO(gzlog, "[raft][%d][%d][pkt:%d][%s]: peer[%d] replicating[%u] "
		   "permit[%d] rejected[%lld] last_log_index[%lld]",
		   rkey(r), r->state, result->pkt, __func__, i, result->pi.replicating,
		   result->pi.permit, result->rejected, result->last_log_index);

    if (r->state != RAFT_LEADER) {
        tracef("local server is not leader -> ignore");
        goto __pgrep_proc;
    }

    rv = recvEnsureMatchingTerms(r, result->term, &match);
    if (rv != 0) {
        goto __pgrep_proc;
    }

    if (match < 0) {
        tracef("local term is higher -> ignore ");
        goto __pgrep_proc;
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
        goto __pgrep_proc;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server = configurationGet(&r->configuration, id);
    if (server == NULL) {
        tracef("unknown server -> ignore");
        goto __pgrep_proc;
    }

	if (result->pi.replicating == PGREP_RND_HRT)
		goto __pgrep_proc;

    /* Update the progress of this server, possibly sending further entries. */
    rv = replicationUpdate(r, server, result);
    if (rv != 0) {
        goto __pgrep_proc;
    }

	pgrep_proc = true;

__pgrep_proc:

	/* Pgrep:
	 *
	 * Release the pgrep permit. And check the returned status.
	 */
	if (result->pi.permit) {

		raft_index prev_applied_index = min(
			max(result->last_log_index, r->log.offset), r->last_applied);
		bool unpermit = false;

		if (!pgrep_proc) {
			ZSINFO(gzlog, "[raft][%d][%d][%s] meet some error pgrep_cancel.",
				   rkey(r), r->state, __func__);
			unpermit = true;
			r->io->pgrep_cancel(r->io);
		} else {
			if (i != r->pgrep_id) {
				/* i is't the pgrep destination. */
				unpermit = true;
			} else if (result->pi.replicating == PGREP_RND_ERR) {
				/* Catch-up meet some error. */
				ZSINFO(gzlog, "[raft][%d][%d][%s] catch-up meet some error pgrep_cancel.",
					   rkey(r), r->state, __func__);
				unpermit = true;
				r->io->pgrep_cancel(r->io);
			} else  if (result->pi.replicating == PGREP_RND_BGN ||
						result->pi.replicating == PGREP_RND_ING) {
				/* Update the prev_applied_index and check to start a new relication. */
				progressUpdateAppliedIndex(r, i, prev_applied_index);
				if (r->last_applied > prev_applied_index) {
					struct pgrep_permit_info pi;
					pi.time = result->pi.time;
					pi.permit = true;
					r->io->pgrep_raft_permit(r->io, RAFT_APD, &pi);
					if (!pi.permit)
						r->io->pgrep_cancel(r->io);
					else
						replicationProgress(r, r->pgrep_id, pi);
				} else {
					unpermit = true;
				}
			}
		}

		if (unpermit) {
			r->io->pgrep_raft_unpermit(r->io, RAFT_APD, &result->pi);
			ZSINFO(gzlog, "[raft][%d][%d][%s]: pgrep permit released.",
				   rkey(r), r->state, __func__);
		}
	}

	if (r->commit_index > r->last_applying &&
		r->last_applied == r->last_applying)
		replicationApply(r, NULL);

    return rv;
}

#undef tracef
