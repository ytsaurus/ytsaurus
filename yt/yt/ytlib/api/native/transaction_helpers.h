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
    virtual void RegisterRequests(int count, bool adjustRequestIndex = false);
    virtual void UnregisterRequests(int count);

    virtual NTransactionClient::TTransactionSignature GenerateSignature();

    // Returns the expected signature to be sent at prepare time.
    // For old-style generators the batches already sum to the target, so this returns 0xffffffffU.
    virtual NTransactionClient::TTransactionSignature GetFinalSignature();

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

    void RegisterRequests(int count, bool adjustRequestIndex = false) override;
    void UnregisterRequests(int count) override;

    NTransactionClient::TTransactionSignature GenerateSignature() override;
    NTransactionClient::TTransactionSignature GetFinalSignature() override;

private:
    //! NB: Best-effort sanity check, not a hard guarantee.
    //! (Un)RegisterRequests are not expected after GenerateSignature, but a violation may slip through;
    //! signatures will be checked later anyway.
    std::atomic<bool> FinalSignatureGenerated_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
