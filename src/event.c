#include "event.h"
#include <assert.h>


static void defaultRecord(void *data, enum raft_event_level level,
			  const char *fn, const char *file, int line,
			  const char *fmt, ...)
{
	(void)data;
	(void)level;
	(void)fn;
	(void)file;
	(void)line;
	(void)fmt;
}

static struct raft_event_recorder defaultRecoder = {
	NULL,
	defaultRecord,
};

static struct raft_event_recorder *currentRecorder = &defaultRecoder;

void raft_set_event_recorder(struct raft_event_recorder *r)
{
	assert(r);
	currentRecorder = r;
}

const struct raft_event_recorder *eventRecorder(void)
{
	return currentRecorder;
}

