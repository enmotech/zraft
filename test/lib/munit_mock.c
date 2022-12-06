/* Copyright (c) 2013-2018 Evan Nemerson <evan@nemerson.com>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "munit_mock.h"
#include "munit.h"
#include <stddef.h>
#include <sys/queue.h>

struct MunitMockItem {
        enum MunitMockType type;
        const char *name;
        union {
                MUNIT_MOCK_RETURN_TYPE val;
                munit_function_fn func;
        };
        TAILQ_ENTRY(MunitMockItem) queue;
};

static TAILQ_HEAD(, MunitMockItem) g_mock_items;

__attribute__((constructor)) static void munit_mock_items_init(void) 
{
        TAILQ_INIT(&g_mock_items);
}

void munit_will_return(const char *name, MUNIT_MOCK_RETURN_TYPE val)
{
        struct MunitMockItem *item = calloc(1, sizeof(*item));

        munit_assert_ptr_not_null(item);
        item->name = name;
        item->type = MUNIT_MOCK_TYPE_RETURN;
        item->val  = val;
        TAILQ_INSERT_TAIL(&g_mock_items, item, queue);
}

void munit_will_call(const char * const name, munit_function_fn func)
{
        struct MunitMockItem *item = calloc(1, sizeof(*item));

        munit_assert_ptr_not_null(item);
        item->name = name;
        item->type = MUNIT_MOCK_TYPE_CALL;
        item->func = func;
        TAILQ_INSERT_TAIL(&g_mock_items, item, queue);
}

#define	TAILQ_FOREACH_SAFE(var, head, field, tvar)			\
	for ((var) = TAILQ_FIRST((head));				\
	    (var) && ((tvar) = TAILQ_NEXT((var), field), 1);		\
	    (var) = (tvar))


static struct MunitMockItem * munit_find_item(const char * const name)
{
        struct MunitMockItem *item;
        struct MunitMockItem *tmp;

        TAILQ_FOREACH_SAFE(item, &g_mock_items, queue, tmp) {
                if (strcmp(item->name, name))
                        continue;
                return item;
        }
        return NULL;
}

enum MunitMockType munit_mock_type(const char * const name)
{
        struct MunitMockItem *item = munit_find_item(name);

        return item ? item->type : MUNIT_MOCK_TYPE_NULL;
}

static struct MunitMockItem * munit_remove_item(const char * const name)
{
        struct MunitMockItem *item;
        struct MunitMockItem *tmp;

        TAILQ_FOREACH_SAFE(item, &g_mock_items, queue, tmp) {
                if (strcmp(item->name, name))
                        continue;
                TAILQ_REMOVE(&g_mock_items, item, queue);
                return item;
        }
        return NULL;
}

MUNIT_MOCK_RETURN_TYPE munit_mock_value(const char * const name)
{
        MUNIT_MOCK_RETURN_TYPE val;
        struct MunitMockItem *item = munit_remove_item(name);

        munit_assert_ptr_not_null(item);
        munit_assert_uint64(item->type, ==, MUNIT_MOCK_TYPE_RETURN);
        val = item->val;
        free(item);
        return val;
}

munit_function_fn munit_mock_function(const char * const name)
{
        munit_function_fn func;
        struct MunitMockItem *item = munit_remove_item(name);

        munit_assert_ptr_not_null(item);
        munit_assert_uint64(item->type, ==, MUNIT_MOCK_TYPE_CALL);
        func = item->func;

        free(item);
        return func;
}
