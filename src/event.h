/*   Utilities for record event in raft */
#ifndef EVENT_H
#define EVENT_H
#include "../include/raft.h"
#include <time.h>
#include <stdint.h>
#include <stdbool.h>

#define EVT_PER_SEC (200)

const struct raft_event_recorder *eventRecorder(void);
#define evtIdAllowed(id) \
	eventRecorder()->is_id_allowed(eventRecorder()->data, id)

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

#define evtErrf(fmt, ...)  evtRecordf(RAFT_ERROR, fmt, __VA_ARGS__)
#define evtWarnf(fmt, ...) evtRecordf(RAFT_WARN, fmt, __VA_ARGS__)

#define evtNoticef(fmt, ...)                                       \
	do {                                                       \
		if (evtLevelAllowed(RAFT_NOTICE))                  \
			evtRecordf(RAFT_NOTICE, fmt, __VA_ARGS__); \
	} while (0)

#define evtInfof(fmt, ...)                                       \
	do {                                                     \
		if (evtLevelAllowed(RAFT_INFO))                  \
			evtRecordf(RAFT_INFO, fmt, __VA_ARGS__); \
	} while (0)

#define evtDebugf(fmt, ...)                                       \
	do {                                                      \
		if (evtLevelAllowed(RAFT_DEBUG))                  \
			evtRecordf(RAFT_DEBUG, fmt, __VA_ARGS__); \
	} while (0)

#define evtIdRecordf(id, level, fmt, ...)                                     \
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
		if (!_skip && evtIdAllowed(id))                               \
			eventRecorder()->record(eventRecorder()->data, level, \
						__func__, __FILE__, __LINE__, \
						fmt, __VA_ARGS__);            \
	} while (0)

#define evtIdErrf(id, fmt, ...)	 evtIdRecordf(id, RAFT_ERROR, fmt, __VA_ARGS__)
#define evtIdWarnf(id, fmt, ...) evtIdRecordf(id, RAFT_WARN, fmt, __VA_ARGS__)

#define evtIdNoticef(id, fmt, ...)                                       \
	do {                                                             \
		if (evtLevelAllowed(RAFT_NOTICE))                        \
			evtIdRecordf(id, RAFT_NOTICE, fmt, __VA_ARGS__); \
	} while (0)

#define evtIdInfof(id, fmt, ...)                                       \
	do {                                                           \
		if (evtLevelAllowed(RAFT_INFO))                        \
			evtIdRecordf(id, RAFT_INFO, fmt, __VA_ARGS__); \
	} while (0)

#define evtIdDebugf(id, fmt, ...)                                       \
	do {                                                            \
		if (evtLevelAllowed(RAFT_DEBUG))                        \
			evtIdRecordf(id, RAFT_DEBUG, fmt, __VA_ARGS__); \
	} while (0)

void evtDumpConfiguration(struct raft *r, const struct raft_configuration *c);

#endif //EVENT_H
