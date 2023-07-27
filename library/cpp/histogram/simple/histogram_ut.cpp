#include "histogram.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>

using namespace NSimpleHistogram;

Y_UNIT_TEST_SUITE(SimpleHistogram) {
    Y_UNIT_TEST(Histogram) {
        THistogramCalcer<size_t> calcer;

        TVector<size_t> values;
        for (size_t i = 0; i < 10000; ++i) {
            values.push_back(RandomNumber<size_t>());
            calcer.RecordValue(values.back());
        }

        THistogram<size_t> hist = calcer.Calc();
        UNIT_ASSERT_VALUES_EQUAL(values.size(), hist.TotalCount());

        Sort(values.begin(), values.end());
        for (size_t j = 0; j < values.size(); ++j) {
            const size_t expectedIndex = static_cast<size_t>((static_cast<double>(j) / values.size()) * values.size());
            const size_t expectedValue = values[expectedIndex];
            const size_t givenValue = hist.ValueAtPercentile(static_cast<double>(j) / values.size());

            UNIT_ASSERT_VALUES_EQUAL(expectedValue, givenValue);
        }
    }

    Y_UNIT_TEST(HistogramWithReserve) {
        THistogramCalcer<size_t> calcer;

        TVector<size_t> values;
        calcer.Reserve(10000);
        values.reserve(10000);
        for (size_t i = 0; i < 10000; ++i) {
            values.push_back(RandomNumber<size_t>());
            calcer.RecordValue(values.back());
        }

        THistogram<size_t> hist = calcer.Calc();
        UNIT_ASSERT_VALUES_EQUAL(values.size(), hist.TotalCount());

        Sort(values.begin(), values.end());
        for (size_t j = 0; j < values.size(); ++j) {
            const size_t expectedIndex = static_cast<size_t>((static_cast<double>(j) / values.size()) * values.size());
            const size_t expectedValue = values[expectedIndex];
            const size_t givenValue = hist.ValueAtPercentile(static_cast<double>(j) / values.size());

            UNIT_ASSERT_VALUES_EQUAL(expectedValue, givenValue);
        }
    }

    Y_UNIT_TEST(JsonHistogram) {
        const TVector<TString> names = {"A", "B", "C"};

        TMultiHistogramCalcer<size_t> calcer;

        for (size_t i = 0; i < names.size(); ++i) {
            for (size_t j = 100 * i; j < 100 * (i + 1); ++j) {
                calcer.RecordValue(names[i], j);
            }
        }

        const TString expected = R"({"A":{"Q50":50,"Q95":95,"RecordCount":100},"B":{"Q50":150,"Q95":195,"RecordCount":100},"C":{"Q50":250,"Q95":295,"RecordCount":100}})";
        const TString given = ToJsonStr(calcer.Calc(), {0.5, 0.95}, false);

        UNIT_ASSERT_STRINGS_EQUAL(expected, given);
    }

    Y_UNIT_TEST(ThreadSafeMultiHistogramCalcer) {
        const TVector<TString> names = {"A", "B", "C"};

        TMultiHistogramCalcer<size_t> calcer;
        TThreadSafeMultiHistogramCalcer<size_t> tsCalcer;

        for (size_t i = 0; i < names.size(); ++i) {
            for (size_t j = 100 * i; j < 100 * (i + 1); ++j) {
                calcer.RecordValue(names[i], j);
            }
        }

        TThreadPool threadPool;
        threadPool.Start(names.size());

        for (size_t i = 0; i < names.size(); ++i) {
            auto worker = [i, &names, &tsCalcer] {
                for (size_t j = 100 * i; j < 100 * (i + 1); ++j) {
                    tsCalcer.RecordValue(names[i], j);
                }
            };

            NThreading::Async(std::move(worker), threadPool);
        }

        threadPool.Stop();

        const TString expected = ToJsonStr(calcer.Calc(), {0.5, 0.95}, false);
        const TString given = ToJsonStr(tsCalcer.Calc(), {0.5, 0.95}, false);

        UNIT_ASSERT_VALUES_EQUAL(expected, given);
    }
}
