#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/errors.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/unittest/registar.h>


using namespace NYT;
using namespace NYT::NTesting;


Y_UNIT_TEST_SUITE(RetryConfigProvider) {
    Y_UNIT_TEST(TestRetriesTimeLimit)
    {
        static constexpr TDuration requestTimeLimit = TDuration::Seconds(3);
        class TTestRetryConfigProvider
            : public IRetryConfigProvider
        {
        public:
            TRetryConfig CreateRetryConfig() override
            {
                TRetryConfig retryConfig;
                retryConfig.RetriesTimeLimit = requestTimeLimit;
                return retryConfig;
            }
        };
        TTestFixture fixture(TCreateClientOptions().RetryConfigProvider(MakeIntrusive<TTestRetryConfigProvider>()));

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryCount = 100;
        TConfig::Get()->RetryInterval = TDuration::MilliSeconds(500);

        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/table", NT_MAP);

        auto outage = TAbortableHttpResponse::StartOutage("/set");
        auto deadline = TInstant::Now() + requestTimeLimit;
        try {
            client->Set(workingDir + "/table/@my_attr", 42);
            UNIT_FAIL("Set() must have been thrown");
        } catch (const TRequestRetriesTimeout&) {
            // It's OK
        }
        auto now = TInstant::Now();
        auto delta = TDuration::Seconds(2);

        UNIT_ASSERT_LT(deadline - delta, now);
        UNIT_ASSERT_LT(now, deadline + delta);
    }
}
