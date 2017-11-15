#include "lib.h"

#include <library/threading/future/async.h>

#include <library/unittest/registar.h>

#include <util/thread/queue.h>

using namespace NYT;
using namespace NYT::NTesting;

template <typename T>
T MakeCopy(const T& t) {
    return t;
}

SIMPLE_UNIT_TEST_SUITE(Concurrency) {
    SIMPLE_UNIT_TEST(TestConcurrency) {
        NYT::Initialize(0, nullptr);
        auto client = CreateTestClient();
        client->Set("//testing/foo", 54);

        auto threadPool = CreateMtpQueue(20);

        const auto writer = [&] {
            for (int i = 0; i != 500; ++i) {
                client->Set("//testing/foo", 42);
            }
        };

        const auto reader = [&] {
            for (int i = 0; i != 500; ++i) {
                client->Get("//testing/foo");
            }
        };

        TVector<NThreading::TFuture<void>> results;
        for (int i = 0; i != 10; ++i) {
            results.emplace_back(NThreading::Async(MakeCopy(writer), *threadPool));
        };
        for (int i = 0; i != 10; ++i) {
            results.emplace_back(NThreading::Async(MakeCopy(reader), *threadPool));
        };

        for (auto& f : results) {
            f.Wait();
        }
    }
}
