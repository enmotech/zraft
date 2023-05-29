#include "../../src/snapshot_sampler.h"
#include "../lib/runner.h"

#define TIME_START  3000
#define TIME_SPAN   1000
#define TIME_PERIOD 10

struct fixture {
	struct raft_snapshot_sampler s;
};

static void *
setUp(MUNIT_UNUSED const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int		rv;

	rv = snapshotSamplerInit(&f->s, TIME_SPAN, TIME_PERIOD, TIME_START);
	munit_assert_int(rv, ==, 0);
	munit_assert_size(f->s.size, ==, 100);
	munit_assert_size(f->s.last, ==, 0);
	munit_assert_uint64(f->s.last_time, ==, TIME_START);

	return f;
}

static void
tearDown(void *data)
{
	struct fixture *f = data;

	snapshotSamplerClose(&f->s);
	free(f);
}

SUITE(snapshotSampler)

TEST(snapshotSampler, takeLessInPeriod, setUp, tearDown, 0, NULL)
{
	struct fixture		     *f = data;
	struct raft_snapshot_sampler *s = &f->s;
	size_t			      last;

	last = s->last;
	snapshotSamplerTake(s, 1, s->last_time + 1);
	munit_assert_uint64(s->last, ==, last);

	last = s->last;
	snapshotSamplerTake(s, 1, s->last_time + TIME_PERIOD - 1);
	munit_assert_uint64(s->last, ==, last);

	return MUNIT_OK;
}

TEST(snapshotSampler, takePeriodByPeriod, setUp, tearDown, 0, NULL)
{
	struct fixture		     *f = data;
	struct raft_snapshot_sampler *s = &f->s;
	size_t			      i;
	size_t			      last;

	for (i = 1; i < s->size; ++i) {
		last = s->last;
		snapshotSamplerTake(s, i, s->last_time + TIME_PERIOD);
		munit_assert_uint64(s->last, ==, last + 1);
		munit_assert_uint64(snapshotSamplerFirstIndex(s), ==, 0);
	}

	last = s->last;
	snapshotSamplerTake(s, i, s->last_time + TIME_PERIOD);
	munit_assert_uint64(s->last, ==, (last + 1) % s->size);
	munit_assert_uint64(snapshotSamplerFirstIndex(s), ==, 1);

	return MUNIT_OK;
}

TEST(snapshotSampler, takeMultiPeriod, setUp, tearDown, 0, NULL)
{
	struct fixture		     *f = data;
	struct raft_snapshot_sampler *s = &f->s;

	snapshotSamplerTake(s, 1, s->last_time + TIME_PERIOD);
	munit_assert_uint64(s->last, ==, 1);
	munit_assert_uint64(snapshotSamplerFirstIndex(s), ==, 0);

	snapshotSamplerTake(s, 2, s->last_time + 2 * TIME_PERIOD);
	munit_assert_uint64(s->last, ==, 3);
	munit_assert_uint64(s->samples[2].index, ==, 1);
	munit_assert_uint64(snapshotSamplerFirstIndex(s), ==, 0);

	snapshotSamplerTake(s, 3, s->last_time + 99 * TIME_PERIOD);
	munit_assert_uint64(s->last, ==, 2);
	munit_assert_uint64(s->samples[2].index, ==, 3);
	munit_assert_uint64(s->samples[1].index, ==, 2);
	munit_assert_uint64(s->samples[3].index, ==, 2);
	munit_assert_uint64(s->samples[4].index, ==, 2);
	munit_assert_uint64(snapshotSamplerFirstIndex(s), ==, 2);

	snapshotSamplerTake(s, 4, s->last_time + 120 * TIME_PERIOD);
	munit_assert_uint64(s->last, ==, 2);
	munit_assert_uint64(s->samples[2].index, ==, 4);
	munit_assert_uint64(s->samples[1].index, ==, 3);
	munit_assert_uint64(s->samples[3].index, ==, 3);
	munit_assert_uint64(s->samples[4].index, ==, 3);
	munit_assert_uint64(snapshotSamplerFirstIndex(s), ==, 3);

	return MUNIT_OK;
}