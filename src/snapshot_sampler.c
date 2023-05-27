#include "snapshot_sampler.h"
#include "assert.h"
#include "event.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

int
snapshotSamplerInit(struct raft_snapshot_sampler *s, raft_time span,
		    raft_time period, raft_time now)
{
	s->span	     = span;
	s->period    = period;
	s->size	     = span / period;
	s->last	     = 0;
	s->last_time = now;

	assert(s->size);
	s->samples = raft_calloc(s->size, sizeof(struct raft_snapshot_sample));
	if (!s->samples) {
		evtErrf("calloc snapshot samples failed %u", s->size);
		return RAFT_NOMEM;
	}
	return 0;
}

void
snapshotSamplerClose(struct raft_snapshot_sampler *s)
{
	assert(s);

	raft_free(s->samples);
	s->samples = NULL;
}

void
snapshotSamplerTake(struct raft_snapshot_sampler *s, raft_index snapshot_index,
		    raft_time time)
{
	assert(s);
	assert(time >= s->last_time);
	size_t diff = (time - s->last_time) / s->period;
	size_t i;
	size_t idx;

	diff = min(diff, s->size);
	for (i = 1; i < diff; ++i) {
		idx			= (s->last + i) % s->size;
		s->samples[idx].index = s->samples[s->last].index;
	}

	if (diff != 0) {
		s->last_time		  = time;
		s->last			  = (s->last + diff) % s->size;
		s->samples[s->last].index = snapshot_index;
	}
}

raft_index
snapshotSamplerFirstIndex(struct raft_snapshot_sampler *s)
{
	size_t idx = (s->last + 1) % s->size;

	return s->samples[idx].index;
}