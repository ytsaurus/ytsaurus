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

    virtual NTransactionClient::TTransactionSignature GenerateSignature();

    // Returns the expected signature to be sent at prepare time.
    // For old-style generators the batches already sum to the target, so this returns 0xffffffffU.
    virtual NTransactionClient::TTransactionSignature GetFinalSignature() const;

    virtual ~TTransactionSignatureGenerator() = default;

protected:
    const NTransactionClient::TTransactionSignature TargetSignature_;

    std::atomic<ui32> RequestCount_ = 0;
    std::atomic<ui32> RequestIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Generates signature 1 per batch; the final signature is sent as ExpectedPrepareSignature at prepare time.
class TUniformSignatureGenerator
    : public TTransactionSignatureGenerator
{
public:
    TUniformSignatureGenerator()
        : TTransactionSignatureGenerator(/*targetSignature*/ 0)
    { }

    NTransactionClient::TTransactionSignature GenerateSignature() override;
    NTransactionClient::TTransactionSignature GetFinalSignature() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
