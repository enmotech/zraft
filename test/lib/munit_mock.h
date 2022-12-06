
/* µnit Testing Framework
 * Copyright (c) 2013-2017 Evan Nemerson <evan@nemerson.com>
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

#if !defined(MUNIT_MOCK_H)
#define MUNIT_MOCK_H
#include <stdint.h>

#define MUNIT_MOCK_RETURN_TYPE uint64_t
typedef void (*munit_function_fn)(void);

enum MunitMockType {
        MUNIT_MOCK_TYPE_NULL = 0,
        MUNIT_MOCK_TYPE_RETURN = 1,
        MUNIT_MOCK_TYPE_CALL = 2
};

void munit_will_return(const char * const name, MUNIT_MOCK_RETURN_TYPE val);
void munit_will_call(const char * const name, munit_function_fn func);

enum MunitMockType munit_mock_type(const char * const name);

MUNIT_MOCK_RETURN_TYPE munit_mock_value(const char * const name);
munit_function_fn munit_mock_function(const char * const name);

#define will_return(func, val) \
        munit_will_return(#func, (val))

#define will_return_ptr(func, val) \
        munit_will_return(#func, (uintptr_t)(val))

#define will_call(func, wrapper) \
        munit_will_call(#func, (munit_function_fn)wrapper)

#define mock_type(type, func) \
        (munit_mock_type(#func) == MUNIT_MOCK_TYPE_NULL) \
                ? (type)__real_##func() \
                : (munit_mock_type(#func) == MUNIT_MOCK_TYPE_RETURN) \
                        ? (type)munit_mock_value(#func) \
                        : (type)((typeof(func) *)munit_mock_function(#func))()


#define mock_type_args(type, func, ...) \
        (munit_mock_type(#func) == MUNIT_MOCK_TYPE_NULL) \
                ? (type)__real_##func(__VA_ARGS__) \
                : (munit_mock_type(#func) == MUNIT_MOCK_TYPE_RETURN) \
                        ? (type)munit_mock_value(#func) \
                        : (type)((typeof(func) *)munit_mock_function(#func))(__VA_ARGS__)



#endif /* !defined(MUNIT_MOCK_H) */