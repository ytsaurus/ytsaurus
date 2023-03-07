#pragma once

#include "public.h"

#include <yt/client/hydra/version.h>

#include <yt/core/actions/callback.h>

#include <yt/core/misc/random.h>
#include <yt/core/misc/ref.h>

#include <yt/core/tracing/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TMutationRequest
{
    TReign Reign = 0;
    TString Type;
    TSharedRef Data;
    TCallback<void(TMutationContext*)> Handler;
    bool AllowLeaderForwarding = false;
    NRpc::TMutationId MutationId;
    bool Retry = false;
};

DEFINE_ENUM(EMutationResponseOrigin,
    (Commit)
    (LeaderForwarding)
    (ResponseKeeper)
);

struct TMutationResponse
{
    EMutationResponseOrigin Origin = EMutationResponseOrigin::Commit;
    TSharedRefArray Data;
};

////////////////////////////////////////////////////////////////////////////////

class TMutationContext
{
public:
    TMutationContext(
        TMutationContext* parent,
        const TMutationRequest& request);

    TMutationContext(
        TVersion version,
        const TMutationRequest& request,
        TInstant timestamp,
        ui64 randomSeed,
        ui64 prevRandomSeed,
        i64 sequenceNumber);

    TVersion GetVersion() const;
    const TMutationRequest& Request() const;
    TInstant GetTimestamp() const;
    ui64 GetRandomSeed() const;
    ui64 GetPrevRandomSeed() const;
    i64 GetSequenceNumber() const;
    TRandomGenerator& RandomGenerator();

    void SetResponseData(TSharedRefArray data);
    const TSharedRefArray& GetResponseData() const;

    void SetResponseKeeperSuppressed(bool value);
    bool GetResponseKeeperSuppressed();

private:
    TMutationContext* const Parent_;
    const TVersion Version_;
    const TMutationRequest& Request_;
    const TInstant Timestamp_;

    TSharedRefArray ResponseData_;
    ui64 RandomSeed_;
    ui64 PrevRandomSeed_;
    i64 SequenceNumber_;
    TRandomGenerator RandomGenerator_;
    bool ResponseKeeperSuppressed_ = false;
};

TMutationContext* TryGetCurrentMutationContext();
TMutationContext* GetCurrentMutationContext();
bool HasMutationContext();
void SetCurrentMutationContext(TMutationContext* context);
TError SanitizeWithCurrentMutationContext(const TError& error);

////////////////////////////////////////////////////////////////////////////////

class TMutationContextGuard
    : public TNonCopyable
{
public:
    explicit TMutationContextGuard(TMutationContext* context);
    ~TMutationContextGuard();

private:
    TMutationContext* Context_;
    TMutationContext* SavedContext_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

