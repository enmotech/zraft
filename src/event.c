#include "event.h"
#include <limits.h>
#include <assert.h>
#include "configuration.h"


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

static bool defaultIsIdAllowed(void *data, raft_id id)
{
	(void)data;
	(void)id;

	return true;
}

static int defaultGetLevel(void *data)
{
	(void)data;

	return RAFT_DEBUG;
}

static struct raft_event_recorder defaultRecoder = {
	NULL,
	defaultIsIdAllowed,
	defaultRecord,
	defaultGetLevel,
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

void evtDumpConfiguration(struct raft *r, const struct raft_configuration *c)
{
	unsigned i;
	int rv;
	size_t offset;
	char buf[PATH_MAX];
	struct raft_server *s;

	offset = (size_t)snprintf(buf, sizeof(buf), "raft(%llx) phase %s ",
				r->id, configurationPhaseName(c->phase));
	for (i = 0; i < c->n; ++i) {
		s = &c->servers[i];
		rv = snprintf(buf + offset, sizeof(buf) - offset,
			      " %llx-%s-%s/%s ", s->id,
			      configurationGroupName(s->group),
			      configurationRoleName(s->role),
			      configurationRoleName(s->role_new));
		assert((size_t)rv < sizeof(buf) - offset);
		offset += (size_t)rv;
	}
	evtNoticef("N-1528-017", "%s", buf);
}
