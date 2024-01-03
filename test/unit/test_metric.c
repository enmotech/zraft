#include "../../src/metric.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture.
 *
 *****************************************************************************/

struct fixture
{
        struct raft_metric m;
};

static void *setUp(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    metricInit(&f->m);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    free(f);
}

/******************************************************************************
 *
 * Metric
 *
 *****************************************************************************/
SUITE(Metric)

TEST(Metric, init, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    munit_assert(f->m.nr_events == 0);
    munit_assert(f->m.nr_samples == 0);
    munit_assert(f->m.latency == 0);

    return MUNIT_OK;
}

TEST(Metric, sample, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t rate = 4;

    // Take first sample
    munit_assert_false(metricShouldSample(&f->m, rate));
    munit_assert_false(metricShouldSample(&f->m, rate));
    munit_assert_false(metricShouldSample(&f->m, rate));
    munit_assert_true(metricShouldSample(&f->m, rate));

    metricSampleLatency(&f->m, 100);
    munit_assert(f->m.latency == 100);

    // Take second sample
    munit_assert_false(metricShouldSample(&f->m, rate));
    munit_assert_false(metricShouldSample(&f->m, rate));
    munit_assert_false(metricShouldSample(&f->m, rate));
    munit_assert_true(metricShouldSample(&f->m, rate));

    metricSampleLatency(&f->m, 200);
    munit_assert(f->m.latency == 150);

    return MUNIT_OK;
}

TEST(Metric, reset, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    metricReset(&f->m);
    munit_assert(f->m.nr_events == 0);
    munit_assert(f->m.nr_samples == 0);
    munit_assert(f->m.latency == 0);

    return MUNIT_OK;
}