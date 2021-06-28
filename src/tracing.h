/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

#include "../include/raft.h"
#include "../../../infrastructure/zlog/logger.h"

#define RAFT_TEST 1 

#ifdef RAFT_TEST
#undef ZSINFO
#undef ZSERROR
#undef ZSWARNING

#define ZSINFO(g, p, ...)
#define ZSERROR(g, p, ...)
#define ZSWARNING(g, p, ...)
#else
extern struct zlogger *gzlog;
#endif

/* Default no-op tracer. */
extern struct raft_tracer NoopTracer;

/* Emit a debug message with the given tracer. */
#define Tracef(TRACER, ...)                             \
    do {                                                \
        char _msg[1024];                                \
        snprintf(_msg, sizeof _msg, __VA_ARGS__);       \
        TRACER->emit(TRACER, __FILE__, __LINE__, _msg); \
    } while (0)

#endif /* TRACING_H_ */
