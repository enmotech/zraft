#include "../../src/request.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

struct fixture
{
    struct request_registry reg;
};

static void *setUp(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    requestRegInit(&f->reg);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    requestRegClose(&f->reg);
    free(f);
}

SUITE(request)

TEST(request, enqueue, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct request r1 = {.index = 1};
    struct request r2 = {.index = 2};
    struct request r8 = {.index = 8};

    requestRegEnqueue(&f->reg, &r1);
    requestRegEnqueue(&f->reg, &r2);
    munit_assert_uint64(2, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r8);
    munit_assert_uint64(8, ==, requestRegNumRequests(&f->reg));
    return MUNIT_OK;
}

TEST(request, del, setUp, tearDown, 0, NULL)
{
    struct request *r;
    struct fixture *f = data;
    struct request r1 = {.index = 1};
    struct request r2 = {.index = 2};
    struct request r4 = {.index = 4};
    struct request r8 = {.index = 8};
    struct request r16 = {.index = 16};

    requestRegEnqueue(&f->reg, &r1);
    requestRegEnqueue(&f->reg, &r2);
    munit_assert_uint64(2, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r4);
    munit_assert_uint64(4, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r8);
    munit_assert_uint64(8, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r16);
    munit_assert_uint64(16, ==, requestRegNumRequests(&f->reg));

    r = requestRegDel(&f->reg, 2);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 2);
    munit_assert_uint64(16, ==, requestRegNumRequests(&f->reg));

    r = requestRegDel(&f->reg, 1);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 1);
    munit_assert_uint64(13, ==, requestRegNumRequests(&f->reg));

    r = requestRegDel(&f->reg, 16);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 16);
    munit_assert_uint64(5, ==, requestRegNumRequests(&f->reg));

    r = requestRegDel(&f->reg, 8);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 8);
    munit_assert_uint64(1, ==, requestRegNumRequests(&f->reg));

    r = requestRegDel(&f->reg, 4);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 4);
    munit_assert_uint64(0, ==, requestRegNumRequests(&f->reg));
    return MUNIT_OK;
}

TEST(request, find, setUp, tearDown, 0, NULL)
{
    struct request *r;
    struct fixture *f = data;
    struct request r1 = {.index = 1};
    struct request r2 = {.index = 2};
    struct request r4 = {.index = 4};
    struct request r8 = {.index = 8};

    requestRegEnqueue(&f->reg, &r1);
    requestRegEnqueue(&f->reg, &r2);
    munit_assert_uint64(2, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r4);
    munit_assert_uint64(4, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r8);
    munit_assert_uint64(8, ==, requestRegNumRequests(&f->reg));

    r = requestRegFind(&f->reg, 2);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 2);

    r = requestRegFind(&f->reg, 1);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 1);

    r = requestRegFind(&f->reg, 8);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 8);

    r = requestRegFind(&f->reg, 4);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 4);
    return MUNIT_OK;
}

TEST(request, dequeue, setUp, tearDown, 0, NULL)
{
    struct request *r;
    struct fixture *f = data;
    struct request r1 = {.index = 1};
    struct request r2 = {.index = 2};
    struct request r4 = {.index = 4};
    struct request r8 = {.index = 8};

    requestRegEnqueue(&f->reg, &r1);
    requestRegEnqueue(&f->reg, &r2);
    munit_assert_uint64(2, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r4);
    munit_assert_uint64(4, ==, requestRegNumRequests(&f->reg));
    requestRegEnqueue(&f->reg, &r8);
    munit_assert_uint64(8, ==, requestRegNumRequests(&f->reg));

    r = requestRegDequeue(&f->reg);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 1);

    r = requestRegDequeue(&f->reg);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 2);

    r = requestRegDequeue(&f->reg);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 4);

    r = requestRegDequeue(&f->reg);
    munit_assert_not_null(r);
    munit_assert_uint64(r->index, ==, 8);
    return MUNIT_OK;
}