/*   Utilities for record event in raft */
#ifndef EVENT_H
#define EVENT_H
#include "../include/raft.h"

const struct raft_event_recorder *eventRecorder(void);


#define evtRecordf(level, fmt, ...)                    \
    do {                                               \
        eventRecorder()->record(eventRecorder()->data, \
	                        level,                 \
				__func__,              \
				__FILE__,              \
				__LINE__,              \
				fmt,                   \
				__VA_ARGS__);          \
    } while(0)

#define evtErrf(fmt, ...)  evtRecordf(RAFT_ERROR, fmt, __VA_ARGS__)
#define evtWarnf(fmt, ...) evtRecordf(RAFT_WARN, fmt, __VA_ARGS__)
#define evtNoticef(fmt, ...) evtRecordf(RAFT_NOTICE, fmt, __VA_ARGS__)
#define evtInfof(fmt, ...) evtRecordf(RAFT_INFO, fmt, __VA_ARGS__)
#define evtDebugf(fmt, ...) evtRecordf(RAFT_DEBUG, fmt, __VA_ARGS__)

#endif //EVENT_H
