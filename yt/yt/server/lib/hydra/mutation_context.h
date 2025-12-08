#pragma once

#include "public.h"
#include "private.h"
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

class TMutationContext
    : public THydraContext
{
public:
    TMutationContext(
        TMutationContext* parent,
        const TMutationRequest* request);

    TMutationContext(
        TLogicalVersion logicalVersion,
        TPhysicalVersion physicalVersion,
        TPhysicalVersion compatOnlyPhysicalVersion,
        const TMutationRequest* request,
        TInstant timestamp,
        ui64 randomSeed,
        ui64 prevRandomSeed,
        i64 sequenceNumber,
        ui64 stateHash,
        int term,
        TSharedRef localHostNameOverride);

    explicit TMutationContext(TTestingTag);

    // NB: Nested mutation contexts rely on the parent to be alive when they
    // themselves are destroyed. Since this class is not refcounted, if a
    // parent class is created on the stack, the lack of copy / move
    // constructors ensures that it will outlive the nested contexts.
    TMutationContext(const TMutationContext& other) = delete;
    TMutationContext(TMutationContext&& other) = delete;

    TMutationContext& operator=(const TMutationContext& other) = delete;
    TMutationContext& operator=(TMutationContext&& other) = delete;

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

    TSharedRefArray ResponseData_;
    ui64 PrevRandomSeed_;
    i64 SequenceNumber_;
    ui64 StateHash_;
    int Term_;
    bool ResponseKeeperSuppressed_ = false;

    int CumulativeVersionDelta_ = 0;

    TLogicalVersion GetLastUsedVersion() const;
    friend TDecoratedAutomaton;
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
    //! Makes all errors inside mutation deterministic.
    TErrorSanitizerGuard ErrorSanitizerGuard_;
    TMutationContext* Context_;
    TMutationContext* SavedContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define MUTATION_CONTEXT_INL_H_
#include "mutation_context-inl.h"
#undef MUTATION_CONTEXT_INL_H_
