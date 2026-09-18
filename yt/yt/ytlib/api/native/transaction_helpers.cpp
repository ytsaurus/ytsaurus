#include "transaction_helpers.h"

namespace NYT::NApi::NNative {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionSignatureGenerator::TTransactionSignatureGenerator(TTransactionSignature targetSignature)
    : TargetSignature_(targetSignature)
{ }

ui64 TTransactionSignatureGenerator::PackState(ui32 requestIndex, ui32 requestCount)
{
    return (static_cast<ui64>(requestIndex) << 32) | requestCount;
}

std::pair<ui32, ui32> TTransactionSignatureGenerator::UnpackState(ui64 state)
{
    return {state >> 32, static_cast<ui32>(state)};
}

void TTransactionSignatureGenerator::RegisterRequest()
{
    RegisterRequests(/*count*/ 1);
}

void TTransactionSignatureGenerator::RegisterRequests(int count, bool /*adjustRequestIndex*/)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto [requestIndex, _] = UnpackState(SignatureGeneratorState_.fetch_add(PackState(0, count)));
    YT_VERIFY(requestIndex == 0);
}

void TTransactionSignatureGenerator::UnregisterRequests(int /*count*/)
{
    YT_UNIMPLEMENTED();
}

TTransactionSignature TTransactionSignatureGenerator::GenerateSignature()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto [requestIndex, requestCount] = UnpackState(
        SignatureGeneratorState_.fetch_add(PackState(1, 0), std::memory_order::relaxed));
    YT_VERIFY(requestIndex < requestCount);

    // NB(gritukan): For now it is not important which request has non-trivial signature
    // but probably property that it is first request will be extremely important in future.
    if (requestIndex == 0) {
        return TargetSignature_ - (requestCount - 1);
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

    auto requestIndexDelta = adjustRequestIndex ? count : 0;
    SignatureGeneratorState_.fetch_add(PackState(requestIndexDelta, count));
}

void TUniformSignatureGenerator::UnregisterRequests(int count)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_ASSERT(!FinalSignatureGenerated_.load());
    YT_VERIFY(count > 0);

    auto [requestIndex, requestCount] = UnpackState(SignatureGeneratorState_.fetch_sub(PackState(count, count)));
    YT_VERIFY(requestIndex >= static_cast<ui32>(count));
    YT_VERIFY(requestCount >= static_cast<ui32>(count));
}

TTransactionSignature TUniformSignatureGenerator::GenerateSignature()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto [requestIndex, requestCount] = UnpackState(
        SignatureGeneratorState_.fetch_add(PackState(1, 0), std::memory_order::relaxed));
    YT_VERIFY(requestIndex < requestCount);

    return 1;
}

TTransactionSignature TUniformSignatureGenerator::GetFinalSignature()
{
    FinalSignatureGenerated_.store(true);

    auto [_, requestCount] = UnpackState(SignatureGeneratorState_.load());
    return requestCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
