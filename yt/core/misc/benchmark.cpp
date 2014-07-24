/*
 * Mostly an adaptation of Benchmark utility from Facebooks' Folly.
 * https://github.com/facebook/folly/blob/master/folly/Benchmark.cpp
 * @ efdc68945456c0c71a05d4a58cf8491454af3ce3
 */
/*
 * Copyright 2012 Facebook, Inc.
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

// @author Andrei Alexandrescu (andrei.alexandrescu@fb.com)

#include "benchmark.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <utility>
#include <vector>

// DEFINE_bool(benchmark, false, "Run benchmarks.");
// DEFINE_bool(json, false, "Output in JSON format.");
// DEFINE_string(bm_regex, "", "Only benchmarks whose names match this regex will be run.");

// Minimum # of microseconds we'll accept for each benchmark.
const ui64 FLAGS_bm_min_usec = 100;
// Minimum # of iterations we'll try for each benchmark.
const ui64 FLAGS_bm_min_iters = 100;
// Maximum # of seconds we'll spend on each benchmark.
const uint32_t FLAGS_bm_max_secs = 15;

using std::min;
using std::max;
using std::get;

namespace NYT {

using namespace NHRTimer;

////////////////////////////////////////////////////////////////////////////////

THRDuration TBenchmarkSuspender::NsSpent;

typedef TCallback< ui64(unsigned int) > TBenchmarkCallback;

typedef std::tuple<const char*, const char*, double> TBenchmarkResultTuple;
typedef std::tuple<const char*, const char*, TBenchmarkCallback> TBenchmarkCallbackTuple;

typedef std::vector<TBenchmarkResultTuple> TBenchmarkResults;
typedef std::vector<TBenchmarkCallbackTuple> TBenchmarkCallbacks;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static inline TBenchmarkCallbacks& GetBenchmarks()
{
    static TBenchmarkCallbacks callbacks;
    return callbacks;
}

void AddBenchmarkImpl(
    const char* file,
    const char* name,
    TBenchmarkCallback function)
{
    GetBenchmarks().emplace_back(file, name, std::move(function));
}

/**
 * Given a bunch of benchmark samples, estimate the actual run time.
 */
static double EstimateTime(double* begin, double * end)
{
    YASSERT(begin < end);

    // Current state of the art: get the minimum. After some
    // experimentation, it seems taking the minimum is the best.
    return *std::min_element(begin, end);
    // XXX(sandello):
    // In `folly` there was some code which was removing outliers and estimating
    // sample mode. However, it is unused. So I removed it.
}

static double RunBenchmarkGetNSPerIteration(
    const TBenchmarkCallback& function,
    const double baseline)
{
    // They key here is accuracy; too low numbers means the accuracy was
    // coarse. We up the ante until we get to at least |minTimeInNs|
    // timings.

    // We do measurements in several epochs and take the minimum, to
    // account for jitter.
    static const unsigned int epochs = 1000;

    // We establish a total time budget as we don't want a measurement
    // to take too long. This will curtail the number of actual epochs.
    static const ui64 maxTimeInNs = FLAGS_bm_max_secs * 1000000000ULL;

    // We choose a minimum minimum (sic) of 100,000 nanoseconds, but if
    // the clock resolution is worse than that, it will be larger. In
    // essence we're aiming at making the quantization noise 0.01%.
    static const ui64 minTimeInNs = max(
        FLAGS_bm_min_usec * 1000ULL,
        min(GetHRResolution() * 100000ULL, 1000000000ULL));

    THRInstant global;
    GetHRInstant(&global);

    double epochResults[epochs] = { 0 };
    size_t actualEpochs = 0;

    for (; actualEpochs < epochs; ++actualEpochs) {
        for (unsigned int n = FLAGS_bm_min_iters; n < (1UL << 30); n *= 2) {
            const auto result = function.Run(n);
            if (result < minTimeInNs) {
                continue;
            }
            // We got an accurate enough timing, done. But only save if
            // smaller than the current result.
            epochResults[actualEpochs] = max(
                0.0,
                double(result) / n - baseline);
            // Done with the current epoch, we got a meaningful timing.
            break;
        }
        THRInstant now;
        GetHRInstant(&now);
        if (GetHRDuration(global, now) >= maxTimeInNs) {
            // No more time budget available.
            ++actualEpochs;
            break;
        }
    }

    // If the benchmark was basically drowned in baseline noise, it's
    // possible it became negative.
    return max(0.0, EstimateTime(epochResults, epochResults + actualEpochs));
}

struct TScaleInfo {
    double threshold;
    const char* suffix;
};

static const TScaleInfo TimeSuffixes[] {
    { 365.25 * 24 * 3600, "years" },
    { 24 * 3600, "days" },
    { 3600, "hours" },
    { 60, "minutes" },
    { 1, "s"  },
    { 1E-3, "ms" },
    { 1E-6, "us" },
    { 1E-9, "ns" },
    { 1E-12, "ps" },
    { 1E-15, "fs" },
    { 0, nullptr },
};

static const TScaleInfo MetricSuffixes[] {
    { 1E24,  "Y" }, // yotta
    { 1E21,  "Z" }, // zetta
    { 1E18,  "X" }, // exa
    { 1E15,  "P" }, // peta
    { 1E12,  "T" }, // terra
    { 1E9,   "G" }, // giga
    { 1E6,   "M" }, // mega
    { 1E3,   "K" }, // kilo
    { 1,     ""  },
    { 1E-3,  "m" }, // milli
    { 1E-6,  "u" }, // micro
    { 1E-9,  "n" }, // nano
    { 1E-12, "p" }, // pico
    { 1E-15, "f" }, // femto
    { 1E-18, "a" }, // atto
    { 1E-21, "z" }, // zepto
    { 1E-24, "y" }, // yocto
    { 0, nullptr },
};

static std::string HumanReadable(
    double value,
    unsigned int decimals,
    const TScaleInfo* scales)
{
    if (std::isinf(value)) {
        return "Inf";
    }
    if (std::isnan(value)) {
        return "NaN";
    }

    const double absoluteValue = fabs(value);
    const TScaleInfo* scale = scales;
    while (absoluteValue < scale[0].threshold && scale[1].suffix != NULL) {
        ++scale;
    }

    const double scaledValue = value / scale->threshold;
    char renderedValue[64] = { 0 };
    snprintf(renderedValue, 64, "%.*f%s", decimals, scaledValue, scale->suffix);

    return renderedValue;
}

static std::string ReadableTime(double value, unsigned int decimals)
{
    return HumanReadable(value, decimals, TimeSuffixes);
}

static std::string ReadableMetric(double value, unsigned int decimals)
{
    return HumanReadable(value, decimals, MetricSuffixes);
}

static void PrintBenchmarkResultsAsTable(const TBenchmarkResults& data)
{
    // Width available.
    static const unsigned int columns = 76;

    // Compute the longest benchmark name.
    size_t longestName = 0;
    for (const auto& benchmark : GetBenchmarks()) {
        longestName = max(longestName, strlen(get<1>(benchmark)));
    }

    // Print a horizontal rule.
    auto separator = [&] (char pad) { puts(std::string(columns, pad).c_str()); };
    auto indent = [&] (size_t length) { printf("%s", std::string(length, ' ').c_str()); };

    // Print header for a file.
    auto header = [&] (const std::string& file) {
        separator('=');
        puts(file.c_str());
        indent(columns - 28);
        puts("relative  time/iter  iters/s");
        separator('=');
    };

    double baselineNsPerIter = std::numeric_limits<double>::max();
    std::string lastFile = "";

    for (auto& datum : data) {
        std::string file = get<0>(datum);
        if (file != lastFile) {
            // This is a new file starting.
            header(file);
            lastFile = file;
        }

        std::string name = get<1>(datum);
        if (name == "-") {
            separator('-');
            continue;
        }

        bool useBaseline;

        if (name[0] == '%') {
            name.erase(0, 1);
            useBaseline = true;
        } else {
            baselineNsPerIter = get<2>(datum);
            useBaseline = false;
        }

        name.resize(columns - 29, ' ');
        auto nsPerIter = get<2>(datum);
        auto secPerIter = nsPerIter / 1E9;
        auto itersPerSec = 1 / secPerIter;

        if (!useBaseline) {
            // Print without baseline.
            printf("%*s           %9s  %7s\n",
                static_cast<int>(name.size()), name.c_str(),
                ReadableTime(secPerIter, 2).c_str(),
                ReadableMetric(itersPerSec, 2).c_str());
        } else {
            // Print with baseline
            auto relative = baselineNsPerIter / nsPerIter * 100.0;
            printf("%*s %7.2f%%  %9s  %7s\n",
                static_cast<int>(name.size()), name.c_str(),
                relative,
                ReadableTime(secPerIter, 2).c_str(),
                ReadableMetric(itersPerSec, 2).c_str());
        }
    }

    separator('=');
}

static void PrintBenchmarkResults(const TBenchmarkResults& data)
{
    // TODO(sandello): Add JSON here.
    PrintBenchmarkResultsAsTable(data);
}

} // namespace NDetail

// Add the global baseline.
BENCHMARK(GlobalBenchmarkBaseline, times)
{
    // Note that we measure internal iterations to avoid inner
    // function calls in arity-1 test wrapper. See benchmark.h
    // for the implementation of AddBenchmark.
    while (times--) {
        asm volatile("");
    }
}

void RunBenchmarks()
{
    using NDetail::GetBenchmarks;
    using NDetail::PrintBenchmarkResults;
    using NDetail::RunBenchmarkGetNSPerIteration;

    auto& benchmarks = GetBenchmarks();

    YCHECK(!benchmarks.empty());

    TBenchmarkResults results;
    results.reserve(benchmarks.size() - 1);

#if 0
    std::unique_ptr<boost::regex> bmRegex;
    if (!FLAGS_bm_regex.empty()) {
        bmRegex.reset(new boost::regex(FLAGS_bm_regex));
    }
#endif

    auto baselineIterator = std::find_if(
        benchmarks.begin(),
        benchmarks.end(),
        [] (const TBenchmarkCallbackTuple& datum) {
            return strcmp(get<1>(datum), "GlobalBenchmarkBaseline") == 0;
        });

    YCHECK(baselineIterator != benchmarks.end());
    auto baseline = RunBenchmarkGetNSPerIteration(get<2>(*baselineIterator), 0);
    benchmarks.erase(baselineIterator);

    // PLEASE KEEP QUIET. MEASUREMENTS IN PROGRESS.

    for (size_t i = 0; i < benchmarks.size(); ++i) {
        const auto& benchmark = benchmarks[i];
        double elapsed = 0.0;

#if 0
        if (bmRegex && !boost::regex_search(get<1>(benchmarks[i]), *bmRegex)) {
            continue;
        }
#endif

        // Do not run separators separators.
        if (strcmp(get<1>(benchmark), "-") != 0) {
            elapsed = RunBenchmarkGetNSPerIteration(get<2>(benchmark), baseline);
        }

        results.emplace_back(get<0>(benchmark), get<1>(benchmark), elapsed);
    }
    // PLEASE MAKE NOISE. MEASUREMENTS DONE.

    PrintBenchmarkResults(results);
}

} // namespace NYT
