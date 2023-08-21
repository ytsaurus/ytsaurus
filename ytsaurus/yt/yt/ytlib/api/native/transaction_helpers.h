#pragma once

#include "public.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSignatureGenerator
{
public:
    explicit TTransactionSignatureGenerator(NTransactionClient::TTransactionSignature targetSignature);

    void RegisterRequest();
    void RegisterRequests(int count);

    NTransactionClient::TTransactionSignature GenerateSignature();

private:
    const NTransactionClient::TTransactionSignature TargetSignature_;

    std::atomic<ui32> RequestCount_ = 0;
    std::atomic<ui32> RequestIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
