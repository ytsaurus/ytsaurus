#pragma once

#include "public.h"

#include "hydra_context.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/random.h>
#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/tracing/public.h>

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
    NTracing::TTraceContextPtr TraceContext;
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
    : public THydraContext
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
        i64 sequenceNumber,
        ui64 stateHash);

    const TMutationRequest& Request() const;
    ui64 GetPrevRandomSeed() const;
    i64 GetSequenceNumber() const;
    ui64 GetStateHash() const;

    void SetResponseData(TSharedRefArray data);
    const TSharedRefArray& GetResponseData() const;

    void SetResponseKeeperSuppressed(bool value);
    bool GetResponseKeeperSuppressed();

    template <class... Ts>
    void CombineStateHash(const Ts&... ks);

private:
    TMutationContext* const Parent_;
    const TMutationRequest& Request_;

    TSharedRefArray ResponseData_;
    ui64 PrevRandomSeed_;
    i64 SequenceNumber_;
    ui64 StateHash_;
    bool ResponseKeeperSuppressed_ = false;
};

TMutationContext* TryGetCurrentMutationContext();
TMutationContext* GetCurrentMutationContext();
bool HasMutationContext();
void SetCurrentMutationContext(TMutationContext* context);

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

#define MUTATION_CONTEXT_INL_H_
#include "mutation_context-inl.h"
#undef MUTATION_CONTEXT_INL_H_
