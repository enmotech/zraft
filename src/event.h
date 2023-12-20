/*   Utilities for record event in raft */
#ifndef EVENT_H
#define EVENT_H
#include "../include/raft.h"
#include <time.h>
#include <stdint.h>
#include <stdbool.h>

#define EVT_NEXT_ID (265)
#define EVT_PER_SEC (200)

const struct raft_event_recorder *eventRecorder(void);

#define evtLevelAllowed(level) \
	((level) <= eventRecorder()->get_level(eventRecorder()->data))

#define evtRecordf(level, fmt, ...)                                           \
	do {                                                                  \
		bool			 _skip = false;                       \
		static __thread uint32_t _log_count;                          \
		static __thread time_t	 _log_time;                           \
		time_t			 _now = time(NULL);                   \
		if (_log_time == _now) {                                      \
			if (++_log_count > EVT_PER_SEC)                       \
				_skip = true;                                 \
		} else {                                                      \
			_log_time  = _now;                                    \
			_log_count = 1;                                       \
		}                                                             \
		if (!_skip)                                                   \
			eventRecorder()->record(eventRecorder()->data, level, \
						__func__, __FILE__, __LINE__, \
						fmt, __VA_ARGS__);            \
	} while (0)

#define evtErrf(id, fmt, ...)  evtRecordf(RAFT_ERROR, id ": " fmt, __VA_ARGS__)
#define evtWarnf(id, fmt, ...) evtRecordf(RAFT_WARN, id ": " fmt, __VA_ARGS__)

#define evtNoticef(id, fmt, ...)                                       \
	do {                                                       \
		if (evtLevelAllowed(RAFT_NOTICE))                  \
			evtRecordf(RAFT_NOTICE, id, ": " fmt, __VA_ARGS__); \
	} while (0)

#define evtInfof(id, fmt, ...)                                       \
	do {                                                     \
		if (evtLevelAllowed(RAFT_INFO))                  \
			evtRecordf(RAFT_INFO, id ": " fmt, __VA_ARGS__); \
	} while (0)

void evtDumpConfiguration(struct raft *r, const struct raft_configuration *c);

#endif //EVENT_H
