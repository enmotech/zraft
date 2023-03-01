#include "joint_consensus.h"
#include "configuration.h"
#include "assert.h"
#include "convert.h"
#include "log.h"
#include "hook.h"
#include "progress.h"
#include "replication.h"
#include "tracing.h"
#include "err.h"
#include "event.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* 调用该接口将新配置存储Cnew，并把新加入的节点作为standby添加到现有的server数组 */
int jointDecodeNewConf(struct raft *r, struct raft_configuration *configuration,
    struct raft_server *servers, unsigned int n)
{
    unsigned int i = 0, j = 0;
    int rv;

    configurationCopy(&r->configuration, configuration);

    for (i = 0; i < configuration->n; i++) {
        configuration->servers[i].c_old = true;
        configuration->servers[i].c_new = false;
    }

    for (i = 0; i < n; i++) {
        for (j = 0;j < configuration->n; j++) {
            if (servers[i].id == configuration->servers[j].id) {
                configuration->servers[j].c_new = true;    
                break;
            }
        }
        if (j == configuration->n) {
            rv = configurationAdd(configuration, servers[i].id, RAFT_SPARE, false, true);
            if (rv != 0) {
                tracef("add configuration error, rv=%d", rv);
                return rv;
            }
            tracef("add new server: id=%lld, configuration->n=%d", servers[i].id, configuration->n);
        }
    }
    return 0;
}

static bool jointConfigurationReachMajority(
    struct raft_configuration *configuration, unsigned int votes_new, unsigned int votes_old)
{
    unsigned int i = 0;
    unsigned int count_new = 0;
    unsigned int count_old = 0;

    for (i = 0; i < configuration->n; i++) {
        if (configuration->servers[i].c_new) {
            count_new++;
        }
        if (configuration->servers[i].c_old) {
            count_old++;
        }
    }

    if (votes_old > count_old/2 && votes_new > votes_new/2) {
        return true;
    }

    return false;
}

bool jointServerIsInCnew(struct raft *r, const struct raft_configuration *c,
                                           const raft_id id)
{
    unsigned int i = 0;

    for (; i < c->n; i++) {
        tracef("id=%lld, cid=%lld", id, c->servers[i].id);
        if (id == c->servers[i].id) {
            return c->servers[i].c_new;
        }
    }

    return false;
}

void jointReplicationQuorum(struct raft *r, const raft_index index)
{
    unsigned int i;
    unsigned int votes_new = 0;
    unsigned int votes_old = 0;

    assert(r->configuration.phase == RAFT_CONF_JOINT);

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
         if (r->leader_state.progress[i].match_index >= index) {
            if (server->c_old) {
                votes_old++;
            }
            if (server->c_new) {
                votes_new++;
            }
        }
    }

    if (jointConfigurationReachMajority(&r->configuration, votes_old, votes_new)) {
        r->commit_index = index;
        tracef("new commit index in joint %llu", r->commit_index); 
        /*
            在4.3章中提到：once the joint consensus has been committed, the system then
            transitions to the new configuration.
        */
        if (index == r->configuration_uncommitted_index) {
            r->configuration_index = index;
            r->configuration_uncommitted_index = 0;
            jointChangePhase(r, NULL, RAFT_CONF_NORMAL);
        }
        
    }
}

static int jointReplicateConfiguration(
    struct raft *r,
    const struct raft_configuration *configuration)
{
    raft_index index; 
    raft_term term = r->current_term;
    int rv; 
    const struct raft_entry *entry;

    /* Index of the entry being appended. */
    index = logLastIndex(&r->log) + 1;

    /* Encode the new configuration and append it to the log. */
    rv = logAppendConfiguration(&r->log, term, configuration); 
    if (rv != 0) {
        evtErrf("raft(%llx) append conf failed %d", r->id, rv);
        goto err;
    }

    evtNoticef("raft(%llx) conf append at index %lu", r->id, index);
    evtDumpConfiguration(r, configuration);

    entry = logGet(&r->log, index);

    assert(entry);
    assert(entry->type == RAFT_CHANGE);
    r->hook->entry_after_append_fn(r->hook, index, entry);
    hookConfChange(r, configuration);

    if (configuration->n != r->configuration.n) {
        rv = progressRebuildArray(r, configuration);
        if (rv != 0) {
            goto err;
        }
    }

    if (configuration != &r->configuration) {
        raft_configuration_close(&r->configuration);
        r->configuration = *configuration;
    }

    /* Start writing the new log entry to disk and send it to the followers. */
    rv = replicationTrigger(r, index); 
    if (rv != 0) {
        /* TODO: restore the old next/match indexes and configuration. */
        goto err_after_log_append;
    }

    r->configuration_uncommitted_index = index;
    evtNoticef("raft(%llx) configuration_uncommitted_index %lld",
		r->id, index);
    tracef("c_uncommitted_index = %lld", index);
    return 0;

err_after_log_append:
    logTruncate(&r->log, index);

err:
    assert(rv != 0);
    return rv;
}

int jointChangePhase(struct raft *r, struct raft_configuration *configuration, enum configuration_phase new_phase)
{
    struct raft_configuration new_conf;
    unsigned int i = 0;
    int rv = 0;

    assert(r->state == RAFT_LEADER);

    if (r->configuration.phase == RAFT_CONF_NORMAL && new_phase == RAFT_CONF_CATCHUP) {
        new_conf = *configuration;
        evtNoticef("raft(%llx) change to catchup", r->id);
    }

    if (r->configuration.phase == RAFT_CONF_CATCHUP && new_phase == RAFT_CONF_JOINT) {
        for (i = 0; i < r->configuration.n; i++) {
            struct raft_server *server = &r->configuration.servers[i];
            server->role = RAFT_VOTER;
        }
        configurationCopy(&r->configuration, &new_conf);
        evtNoticef("raft(%llx) change to joint", r->id);
    }

    if (r->configuration.phase == RAFT_CONF_JOINT && new_phase == RAFT_CONF_NORMAL) {
        configurationInit(&new_conf);
        for (i = 0; i < r->configuration.n; i++) {
            struct raft_server *server = &r->configuration.servers[i];
            if (server->c_new) {
                configurationAdd(&new_conf, server->id, RAFT_VOTER, server->c_old, server->c_new);
            }
        }
        evtNoticef("raft(%llx) change to C new", r->id);
    }

    new_conf.phase = new_phase;
    rv = jointReplicateConfiguration(r, &new_conf);
    if (rv) {
        evtWarnf("raft(%llx) joint replication error", r->id);
        goto error;
    }
    return 0;
error:
    return RAFT_NOTLEADER;
}

static unsigned jointGetTotalOldServers(struct raft *r)
{
    unsigned i = 0;
    unsigned res = 0;
    
    for (; i < r->configuration.n; i++) {
        if (r->configuration.servers[i].c_old)
            res++;
    }
    return res;
}

static unsigned jointGetTotalNewServers(struct raft *r)
{
    unsigned i = 0;
    unsigned res = 0;
    
    for (; i < r->configuration.n; i++) {
        if (r->configuration.servers[i].c_new)
            res++;
    }
    return res; 
}

bool jointElectionTally(struct raft *r)
{
    size_t n_voters = configurationVoterCount(&r->configuration);
    size_t i;
    size_t half_new = 0;
    size_t half_old = 0;
    size_t votes_new = 0;
    size_t votes_old = 0;

    assert(r->configuration.phase == RAFT_CONF_JOINT);
    for (i = 0; i < n_voters; i++) {
        if (r->candidate_state.votes[i]) {
            if (r->configuration.servers[i].c_new)
                votes_new++;
            if (r->configuration.servers[i].c_old)
                votes_old++;
        }
    }
    half_old = jointGetTotalOldServers(r) / 2;
    half_new = jointGetTotalNewServers(r) / 2;

    return votes_old >= half_old + 1 && votes_new >= half_new + 1;

}

unsigned int jointNQuorum(struct raft *r)
{
    unsigned int q1=0, q2=0;

    for (unsigned int i = 0; i < r->configuration.n; i++) {
        const struct raft_server *server = &r->configuration.servers[i];
        if (server->role == RAFT_SPARE) {
            continue;
        }
        if (server->c_new)
            q1++;
        if (server->c_old)
            q2++;
    }
        
    return q1 > q2 ? q1 : q2;
}

unsigned jointConfigurationVoterCount(const struct raft_configuration *c)
{
    unsigned i;
    unsigned new = 0, old = 0;
    assert(c != NULL);

    for (i = 0; i < c->n; i++) {
        if (c->servers[i].role == RAFT_VOTER) {
            if (c->servers[i].c_new)
                new++;
            if (c->servers[i].c_old)
                old++;
        }
    }
    return new > old ? new:old;
}