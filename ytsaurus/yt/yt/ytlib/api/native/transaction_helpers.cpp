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

void TTransactionSignatureGenerator::RegisterRequests(int count)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YT_VERIFY(RequestIndex_ == 0);

    RequestCount_ += count;
}

TTransactionSignature TTransactionSignatureGenerator::GenerateSignature()
{
    VERIFY_THREAD_AFFINITY_ANY();

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
