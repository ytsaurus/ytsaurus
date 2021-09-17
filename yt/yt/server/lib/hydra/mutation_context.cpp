#include "mutation_context.h"

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    TMutationContext* parent,
    const TMutationRequest& request)
    : THydraContext(*parent)
    , Parent_(parent)
    , Request_(request)
    , PrevRandomSeed_(Parent_->GetPrevRandomSeed())
    , SequenceNumber_(Parent_->GetSequenceNumber())
    , StateHash_(Parent_->GetStateHash())
{ }

TMutationContext::TMutationContext(
    TVersion version,
    const TMutationRequest& request,
    TInstant timestamp,
    ui64 randomSeed,
    ui64 prevRandomSeed,
    i64 sequenceNumber,
    ui64 stateHash)
    : THydraContext(
        version,
        timestamp,
        randomSeed)
    , Parent_(nullptr)
    , Request_(request)
    , PrevRandomSeed_(prevRandomSeed)
    , SequenceNumber_(sequenceNumber)
    , StateHash_(stateHash)
{ }

const TMutationRequest& TMutationContext::Request() const
{
    return Request_;
}

ui64 TMutationContext::GetPrevRandomSeed() const
{
    return PrevRandomSeed_;
}

i64 TMutationContext::GetSequenceNumber() const
{
    return SequenceNumber_;
}

ui64 TMutationContext::GetStateHash() const
{
    return StateHash_;
}

void TMutationContext::SetResponseData(TSharedRefArray data)
{
    ResponseData_ = std::move(data);
}

const TSharedRefArray& TMutationContext::GetResponseData() const
{
    return ResponseData_;
}

void TMutationContext::SetResponseKeeperSuppressed(bool value)
{
    ResponseKeeperSuppressed_ = value;
}

bool TMutationContext::GetResponseKeeperSuppressed()
{
    return ResponseKeeperSuppressed_;
}

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFls<TMutationContext*> CurrentMutationContext;

TMutationContext* TryGetCurrentMutationContext()
{
    return *CurrentMutationContext;
}

TMutationContext* GetCurrentMutationContext()
{
    auto* context = TryGetCurrentMutationContext();
    YT_ASSERT(context);
    return context;
}

bool HasMutationContext()
{
    return TryGetCurrentMutationContext() != nullptr;
}

void SetCurrentMutationContext(TMutationContext* context)
{
    *CurrentMutationContext = context;
    SetCurrentHydraContext(context);
}

////////////////////////////////////////////////////////////////////////////////

TMutationContextGuard::TMutationContextGuard(TMutationContext* context)
    : Context_(context)
    , SavedContext_(TryGetCurrentMutationContext())
{
    YT_ASSERT(Context_);
    SetCurrentMutationContext(Context_);
}

TMutationContextGuard::~TMutationContextGuard()
{
    YT_ASSERT(GetCurrentMutationContext() == Context_);
    SetCurrentMutationContext(SavedContext_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
