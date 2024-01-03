#include "../include/raft.h"
#include <limits.h>
#include "assert.h"
#include "tracing.h"

#ifdef ENABLE_TRACE
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

void metricInit(struct raft_metric *m)
{
        m->nr_events = 0;
        m->nr_samples = 0;
        m->latency = 0;
}

bool metricShouldSample(struct raft_metric *m, size_t rate)
{
        assert((rate & (rate - 1)) == 0);
        m->nr_events += 1;

        return rate == 0 || (m->nr_events & (rate - 1)) == 0;
}

void metricSampleLatency(struct raft_metric *m, raft_time latency)
{
        int avg = (int)m->latency;
        int delta = (int)latency - (int)m->latency;

        m->nr_samples += 1;
        m->latency = (raft_time)(avg + delta / (int)m->nr_samples);

        /* To prevent integer overflow */
        if (m->nr_samples == INT_MAX) {
                m->nr_samples = 1;
        }
}

void metricReset(struct raft_metric *m)
{
        metricInit(m);
}
