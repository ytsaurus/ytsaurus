#include <mapreduce/yt/raw_client/raw_batch_request.h>

#include <mapreduce/yt/http/context.h>
#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/common/retry_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NYT::NDetail::NRawClient;


class TTestRetryPolicy
    : public IRequestRetryPolicy
{
private:
    static constexpr int RetriableCode = 904;

public:
    void NotifyNewAttempt() override
    { }

    TMaybe<TDuration> OnGenericError(const std::exception& /*e*/) override
    {
        return TDuration::Seconds(42);
    }

    void OnIgnoredError(const TErrorResponse& /*e*/) override
    { }

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        if (e.GetError().GetCode() == RetriableCode) {
            return TDuration::Seconds(e.GetError().GetAttributes().at("retry_interval").AsUint64());
        } else {
            return Nothing();
        }
    }

    TString GetAttemptDescription() const override
    {
        return "attempt";
    }

    static TNode GenerateRetriableError(TDuration retryDuration)
    {
        Y_VERIFY(retryDuration - TDuration::Seconds(retryDuration.Seconds()) == TDuration::Zero());

        return TNode()
            ("code", RetriableCode)
            ("attributes",
                TNode()
                    ("retry_interval", retryDuration.Seconds()));
    }
};


TString GetPathFromRequest(const TNode& params)
{
    return params.AsMap().at("parameters").AsMap().at("path").AsString();
}

TVector<TString> GetAllPathsFromRequestList(const TNode& requestList)
{
    TVector<TString> result;
    for (const auto& request : requestList.AsList()) {
        result.push_back(GetPathFromRequest(request)); }
    return result;
}


Y_UNIT_TEST_SUITE(BatchRequestImpl) {
    Y_UNIT_TEST(ParseResponse) {
        TClientContext context;
        TRawBatchRequest batchRequest(context.Config);

        UNIT_ASSERT_VALUES_EQUAL(batchRequest.BatchSize(), 0);

        auto get1 = batchRequest.Get(
            TTransactionId(),
            "//getOk",
            TGetOptions());

        auto get2 = batchRequest.Get(
            TTransactionId(),
            "//getError-3",
            TGetOptions());

        auto get3 = batchRequest.Get(
            TTransactionId(),
            "//getError-5",
            TGetOptions());

        UNIT_ASSERT_VALUES_EQUAL(batchRequest.BatchSize(), 3);

        auto testRetryPolicy = MakeIntrusive<TTestRetryPolicy>();
        const TInstant now = TInstant::Seconds(100500);

        TRawBatchRequest retryBatch(context.Config);
        batchRequest.ParseResponse(
            TNode()
                .Add(TNode()("output", 5))
                .Add(TNode()("error",
                        TTestRetryPolicy::GenerateRetriableError(TDuration::Seconds(3))))
                .Add(TNode()("error",
                        TTestRetryPolicy::GenerateRetriableError(TDuration::Seconds(5)))),
                "<no-request-id>",
                testRetryPolicy,
                &retryBatch,
                now);

        UNIT_ASSERT_VALUES_EQUAL(batchRequest.BatchSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(retryBatch.BatchSize(), 2);

        TNode retryParameterList;
        TInstant nextTry;
        retryBatch.FillParameterList(3, &retryParameterList, &nextTry);
        UNIT_ASSERT_VALUES_EQUAL(
            GetAllPathsFromRequestList(retryParameterList),
            TVector<TString>({"//getError-3", "//getError-5"}));

        UNIT_ASSERT_VALUES_EQUAL(nextTry, now + TDuration::Seconds(5));
    }
}
