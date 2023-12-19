#pragma once

#include "public.h"
#include "hydra_context.h"

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/test_framework/testing_tag.h>

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/ref.h>

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
    TEpochId EpochId;
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

TString MockHostSanitize(TStringBuf);

////////////////////////////////////////////////////////////////////////////////

class TMutationContext
    : public THydraContext
{
public:
    TMutationContext(
        TMutationContext* parent,
        const TMutationRequest* request);

    TMutationContext(
        TVersion version,
        const TMutationRequest* request,
        TInstant timestamp,
        ui64 randomSeed,
        ui64 prevRandomSeed,
        i64 sequenceNumber,
        ui64 stateHash,
        int term,
        TErrorSanitizerGuard::THostNameSanitizer hostNameSanitizer);

    explicit TMutationContext(TTestingTag);

    const TMutationRequest& Request() const;
    ui64 GetPrevRandomSeed() const;
    i64 GetSequenceNumber() const;
    ui64 GetStateHash() const;
    int GetTerm() const;

    //! Used by ResetStateHash mutation.
    //! Probably you do not want to use it in other contexts.
    void SetStateHash(ui64 newStateHash);

    void SetResponseData(TSharedRefArray data);
    void SetResponseData(TError error);
    const TSharedRefArray& GetResponseData() const;
    TSharedRefArray TakeResponseData();

    void SetResponseKeeperSuppressed(bool value);
    bool GetResponseKeeperSuppressed();

    template <class... Ts>
    void CombineStateHash(const Ts&... ks);

private:
    TMutationContext* const Parent_;
    const TMutationRequest* const Request_;
    const TErrorSanitizerGuard::THostNameSanitizer HostNameSanitizer_;

    TSharedRefArray ResponseData_;
    ui64 PrevRandomSeed_;
    i64 SequenceNumber_;
    ui64 StateHash_;
    int Term_;
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
