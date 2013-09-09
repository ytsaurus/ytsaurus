/*
 * Mostly an adaptation of Benchmark utility from Facebooks' Folly.
 * https://github.com/facebook/folly/blob/master/folly/Benchmark.h
 * @ 86d219c38847965714df9eba03b07fa2f6e30ecc
 */
/*
 * Copyright 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "common.h"

#include <core/actions/callback.h>
#include <core/misc/assert.h>
#include <core/misc/hr_timer.h>

#include <functional>
#include <limits>

namespace NYT {

/**
 * Runs all benchmarks defined. Usually put in main().
 */
void RunBenchmarks();

namespace NDetail {

/**
 * Adds a benchmark wrapped in a TCallback.
 * Only used internally. Pass by value is intentional.
 */
void AddBenchmarkImpl(
    const char* file,
    const char* name,
    TCallback<ui64(unsigned int)>);

} // namespace NDetail

/**
 * Supporting type for BENCHMARK_SUSPEND defined below.
 */
struct TBenchmarkSuspender
{
    TBenchmarkSuspender()
    {
        NHRTimer::GetHRInstant(&Begin);
    }

#ifndef _MSC_VER
    TBenchmarkSuspender(const TBenchmarkSuspender &) = delete;
#endif
    TBenchmarkSuspender(TBenchmarkSuspender&& rhs)
    {
        Begin = rhs.Begin;
        rhs.Begin.Seconds = rhs.Begin.Nanoseconds = 0;
    }

#ifndef _MSC_VER
    TBenchmarkSuspender& operator=(const TBenchmarkSuspender &) = delete;
#endif
    TBenchmarkSuspender& operator=(TBenchmarkSuspender&& rhs)
    {
        if (Begin.Nanoseconds > 0 || Begin.Seconds > 0) {
            Tally();
        }
        Begin = rhs.Begin;
        rhs.Begin.Seconds = rhs.Begin.Nanoseconds = 0;
        return *this;
    }

    ~TBenchmarkSuspender()
    {
        if (Begin.Nanoseconds > 0 || Begin.Seconds > 0) {
            Tally();
        }
    }

    void Dismiss()
    {
        YASSERT(Begin.Nanoseconds > 0 || Begin.Seconds > 0);
        Tally();
        Begin.Seconds = Begin.Nanoseconds = 0;
    }

    void Rehire()
    {
        YASSERT(Begin.Nanoseconds > 0 || Begin.Seconds > 0);
        NHRTimer::GetHRInstant(&Begin);
    }

    /**
     * This helps the macro definition. To get around the dangers of
     * operator bool, returns a pointer to member (which allows no
     * arithmetic).
     */
    operator int TBenchmarkSuspender::*() const
    {
        return nullptr;
    }

    /**
     * Accumulates nanoseconds spent outside benchmark.
     */
    static NHRTimer::THRDuration NsSpent;

private:
    void Tally()
    {
        using namespace NHRTimer;

        THRInstant End;
        GetHRInstant(&End);
        NsSpent += GetHRDuration(Begin, End);
    }

    NHRTimer::THRInstant Begin;
};

/**
 * Adds a benchmark. Usually not called directly but instead through
 * the macro BENCHMARK defined below. The lambda function involved
 * must take exactly one parameter of type unsigned, and the benchmark
 * uses it with counter semantics (iteration occurs inside the
 * function).
 */
template <typename TLambda>
typename NMpl::TEnableIfC<
    NDetail::TRunnableAdapter<decltype(&TLambda::operator())>::Arity == 2
>::TType
AddBenchmark(const char* file, const char* name, TLambda&& lambda) {
    auto execute = BIND([=] (unsigned int times) -> ui64 {
        using namespace NHRTimer;

        TBenchmarkSuspender::NsSpent = 0;
        THRInstant begin, end;

        // CORE MEASUREMENT STARTS
        GetHRInstant(&begin);
        lambda(times);
        GetHRInstant(&end);
        // CORE MEASUREMENT ENDS

        return GetHRDuration(begin, end) - TBenchmarkSuspender::NsSpent;
    });

    NDetail::AddBenchmarkImpl(file, name, std::move(execute));
}

/**
 * Adds a benchmark. Usually not called directly but instead through
 * the macro BENCHMARK defined below. The lambda function involved
 * must take zero parameters, and the benchmark calls it repeatedly
 * (iteration occurs outside the function).
 */
template <typename TLambda>
typename NMpl::TEnableIfC<
    NDetail::TRunnableAdapter<decltype(&TLambda::operator())>::Arity == 1
>::TType
AddBenchmark(const char* file, const char* name, TLambda&& lambda) {
    AddBenchmark(file, name, [=] (unsigned int times) {
        while (times-- > 0) {
            lambda();
        }
    });
}

/**
 * Adds a benchmark with a custom timing. This is quite advanced usage
 * for those who would like to measure time spent on their own.
 */
template <typename TLambda>
void AddBenchmarkCustom(
    const char* file,
    const char* name,
    TLambda&& lambda)
{
    NDetail::AddBenchmarkImpl(file, name, BIND(std::move(lambda)));
}

/**
 * Call DoNotOptimizeAway(variable) against variables that you use for
 * benchmarking but otherwise are useless. The compiler tends to do a
 * good job at eliminating unused variables, and this function fools
 * it into thinking var is in fact needed.
 */
template <class T>
void DoNotOptimizeAway(T&& datum) {
    asm volatile("" : "+r" (datum));
}

} // namespace NYT

/**
 * Introduces a benchmark function. Used internally, see BENCHMARK and
 * friends below.
 */
#define BENCHMARK_IMPL(function, name, argType, argName)        \
    static void follyBenchmarkFn##function(argType);            \
    static bool PP_ANONYMOUS_VARIABLE(follyBenchmarkUnused) = ( \
        ::NYT::AddBenchmark(                                    \
            __FILE__,                                           \
            name,                                               \
            [] (argType argName) {                              \
                follyBenchmarkFn##function(argName);            \
            }),                                                 \
        true);                                                  \
    static void follyBenchmarkFn##function(argType argName)

/**
 * Introduces a benchmark function. Used internally, see BENCHMARK and
 * friends below.
 */
#define BENCHMARK_CUSTOM_IMPL(function, name, argType, argName) \
    static ui64 follyBenchmarkFn##function(argType);            \
    static bool PP_ANONYMOUS_VARIABLE(follyBenchmarkUnused) = ( \
        ::NYT::AddBenchmarkCustom(                              \
            __FILE__,                                           \
            name,                                               \
            [] (argType argName) -> ui64 {                      \
                return follyBenchmarkFn##function(argName);     \
            }),                                                 \
        true);                                                  \
    static ui64 follyBenchmarkFn##function(argType argName)

/**
 * Introduces a benchmark function. Use with either one one or two
 * arguments. The first is the name of the benchmark. Use something
 * descriptive, such as insertVectorBegin. The second argument may be
 * missing, or could be a symbolic counter. The counter dictates how
 * many internal iteration the benchmark does. Example:
 *
 * BENCHMARK(vectorPushBack) {
 *   vector<int> v;
 *   v.push_back(42);
 * }
 *
 * BENCHMARK(insertVectorBegin, n) {
 *   vector<int> v;
 *   FOR_EACH_RANGE (i, 0, n) {
 *       v.insert(v.begin(), 42);
 *   }
 * }
 */
#define BENCHMARK(name, ...)                      \
    BENCHMARK_IMPL(                               \
        name,                                     \
        PP_STRINGIZE(name),                       \
        PP_ONE_OR_NONE(unsigned, ## __VA_ARGS__), \
        __VA_ARGS__)

/**
 * Defines a benchmark that passes a parameter to another one. This is
 * common for benchmarks that need a "problem size" in addition to
 * "number of iterations". Consider:
 *
 * void pushBack(unsigned n, size_t initialSize) {
 *   vector<int> v;
 *   BENCHMARK_SUSPEND {
 *       v.resize(initialSize);
 *   }
 *   FOR_EACH_RANGE (i, 0, n) {
 *      v.push_back(i);
 *   }
 * }
 * BENCHMARK_PARAM(pushBack, 0)
 * BENCHMARK_PARAM(pushBack, 1000)
 * BENCHMARK_PARAM(pushBack, 1000000)
 *
 * The benchmark above estimates the speed of push_back at different
 * initial sizes of the vector. The framework will pass 0, 1000, and
 * 1000000 for initialSize, and the iteration count for n.
 */
#define BENCHMARK_PARAM(name, param) \
    BENCHMARK_NAMED_PARAM(name, param, param)

/*
 * Like BENCHMARK_PARAM(), but allows a custom name to be specified for each
 * parameter, rather than using the parameter value.
 *
 * Useful when the parameter value is not a valid token for string pasting,
 * of when you want to specify multiple parameter arguments.
 *
 * For example:
 *
 * void addValue(unsigned n, i64 bucketSize, i64 min, i64 max) {
 *   Histogram<i64> hist(bucketSize, min, max);
 *   i64 num = min;
 *   FOR_EACH_RANGE (i, 0, n) {
 *       hist.addValue(num);
 *       ++num;
 *       if (num > max) { num = min; }
 *   }
 * }
 *
 * BENCHMARK_NAMED_PARAM(addValue, 0_to_100, 1, 0, 100)
 * BENCHMARK_NAMED_PARAM(addValue, 0_to_1000, 10, 0, 1000)
 * BENCHMARK_NAMED_PARAM(addValue, 5k_to_20k, 250, 5000, 20000)
 */
#define BENCHMARK_NAMED_PARAM(name, paramName, ...)         \
    BENCHMARK_IMPL(                                         \
        name##_##paramName,                                 \
        PP_STRINGIZE(name) "(" PP_STRINGIZE(paramName) ")", \
        unsigned,                                           \
        iterations)                                         \
    {                                                       \
        name(iterations, ## __VA_ARGS__);                   \
    }

/**
 * Just like BENCHMARK, but prints the time relative to a
 * baseline. The baseline is the most recent BENCHMARK() seen in
 * lexical order. Example:
 *
 * // This is the baseline
 * BENCHMARK(insertVectorBegin, n) {
 *   vector<int> v;
 *   FOR_EACH_RANGE (i, 0, n) {
 *       v.insert(v.begin(), 42);
 *   }
 * }
 *
 * BENCHMARK_RELATIVE(insertListBegin, n) {
 *   list<int> s;
 *   FOR_EACH_RANGE (i, 0, n) {
 *       s.insert(s.begin(), 42);
 *   }
 * }
 *
 * Any number of relative benchmark can be associated with a
 * baseline. Another BENCHMARK() occurrence effectively establishes a
 * new baseline.
 */
#define BENCHMARK_RELATIVE(name, ...)             \
    BENCHMARK_IMPL(                               \
        name,                                     \
        "%" PP_STRINGIZE(name),                   \
        PP_ONE_OR_NONE(unsigned, ## __VA_ARGS__), \
        __VA_ARGS__)

/**
 * A combination of BENCHMARK_RELATIVE and BENCHMARK_PARAM.
 */
#define BENCHMARK_RELATIVE_PARAM(name, param) \
    BENCHMARK_RELATIVE_NAMED_PARAM(name, param, param)

/**
 * A combination of BENCHMARK_RELATIVE and BENCHMARK_NAMED_PARAM.
 */
#define BENCHMARK_RELATIVE_NAMED_PARAM(name, paramName, ...)    \
    BENCHMARK_IMPL(                                             \
        name##_##paramName,                                     \
        "%" PP_STRINGIZE(name) "(" PP_STRINGIZE(paramName) ")", \
        unsigned,                                               \
        iterations)                                             \
    {                                                           \
        name(iterations, ## __VA_ARGS__);                       \
    }

/**
 * Just like BENCHMARK, but requires _you_ to return a number of
 * nanoseconds passed. You can use NHRTimer for your convenience.
 */
#define BENCHMARK_CUSTOM(name, ...)               \
    BENCHMARK_CUSTOM_IMPL(                        \
        name,                                     \
        PP_STRINGIZE(name),                       \
        PP_ONE_OR_NONE(unsigned, ## __VA_ARGS__), \
        __VA_ARGS__)

/**
 * A combination of BENCHMARK_CUSTOM and BENCHMARK_PARAM.
 */
#define BENCHMARK_CUSTOM_PARAM(name, param) \
    BENCHMARK_CUSTOM_NAMED_PARAM(name, param, param)

/**
 * A combination of BENCHMARK_CUSTOM and BENCHMARK_NAMED_PARAM.
 */
#define BENCHMARK_CUSTOM_NAMED_PARAM(name, paramName, ...)  \
    BENCHMARK_CUSTOM_IMPL(                                  \
        name##_##paramName,                                 \
        PP_STRINGIZE(name) "(" PP_STRINGIZE(paramName) ")", \
        unsigned,                                           \
        iterations)                                         \
    {                                                       \
        return name(iterations, ## __VA_ARGS__);            \
    }

/**
 * Draws a line of dashes.
 */
#define BENCHMARK_DRAW_LINE()                                   \
    static bool PP_ANONYMOUS_VARIABLE(follyBenchmarkUnused) = ( \
        ::NYT::AddBenchmark(__FILE__, "-", [] () {}),           \
        true);

/**
 * Allows execution of code that doesn't count torward the benchmark's
 * time budget. Example:
 *
 * BENCHMARK_START_GROUP(insertVectorBegin, n) {
 *   vector<int> v;
 *   BENCHMARK_SUSPEND {
 *       v.reserve(n);
 *   }
 *   FOR_EACH_RANGE (i, 0, n) {
 *       v.insert(v.begin(), 42);
 *   }
 * }
 */
#define BENCHMARK_SUSPEND                                   \
    if (auto PP_ANONYMOUS_VARIABLE(follyBenchmarkSuspend) = \
        ::NYT::TBenchmarkSuspender())                       \
    { } else
