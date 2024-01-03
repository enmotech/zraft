#ifndef RAFT_METRIC_H_
#define RAFT_METRIC_H_
#include "../include/raft.h"

/**
 * Init metric.
 */
void metricInit(struct raft_metric *m);

/**
 * Verify whether sampling is required or not.
 */
bool metricShouldSample(struct raft_metric *m, size_t rate);

/**
 * Take sample for latency metirc.
 */
void metricSampleLatency(struct raft_metric *m, raft_time latency);

/**
 * Reset metric data.
 */
void metricReset(struct raft_metric *m);

#endif//RAFT_METRIC_H_
