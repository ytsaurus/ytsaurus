#include "transaction_helpers.h"

namespace NYT::NApi::NNative {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionSignatureGenerator::TTransactionSignatureGenerator(TTransactionSignature targetSignature)
    : TargetSignature_(targetSignature)
{ }

void TTransactionSignatureGenerator::RegisterRequest()
{
    RegisterRequests(/*count*/ 1);
}

void TTransactionSignatureGenerator::RegisterRequests(int count, bool /*adjustRequestIndex*/)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(RequestIndex_ == 0);

    RequestCount_ += count;
}

void TTransactionSignatureGenerator::UnregisterRequests(int /*count*/)
{
    YT_UNIMPLEMENTED();
}

TTransactionSignature TTransactionSignatureGenerator::GenerateSignature()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto requestIndex = RequestIndex_.fetch_add(1, std::memory_order::relaxed);
    YT_VERIFY(requestIndex < RequestCount_);

    // NB(gritukan): For now it is not important which request has non-trivial signature
    // but probably property that it is first request will be extremely important in future.
    if (requestIndex == 0) {
        return TargetSignature_ - (RequestCount_ - 1);
    } else {
        return 1;
    }
}

TTransactionSignature TTransactionSignatureGenerator::GetFinalSignature()
{
    return NTransactionClient::FinalTransactionSignature;
}

////////////////////////////////////////////////////////////////////////////////

void TUniformSignatureGenerator::RegisterRequests(int count, bool adjustRequestIndex)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_ASSERT(!FinalSignatureGenerated_.load());
    RequestCount_ += count;

    if (adjustRequestIndex) {
        RequestIndex_ += count;
    }
}

void TUniformSignatureGenerator::UnregisterRequests(int count)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_ASSERT(!FinalSignatureGenerated_.load());
    YT_VERIFY(count > 0);

    YT_VERIFY(RequestIndex_.fetch_sub(count) >= static_cast<unsigned>(count));
    YT_VERIFY(RequestCount_.fetch_sub(count) >= static_cast<unsigned>(count));
}

TTransactionSignature TUniformSignatureGenerator::GenerateSignature()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto requestIndex = RequestIndex_.fetch_add(1, std::memory_order::relaxed);
    YT_VERIFY(requestIndex < RequestCount_);

    return 1;
}

TTransactionSignature TUniformSignatureGenerator::GetFinalSignature()
{
    FinalSignatureGenerated_.store(true);
    return RequestCount_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
