#include "mutation_context.h"

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/message.h>

namespace NYT::NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    TMutationContext* parent,
    const TMutationRequest* request)
    : THydraContext(
        parent->GetVersion(),
        parent->GetTimestamp(),
        parent->GetRandomSeed(),
        parent->RandomGenerator())
    , Parent_(parent)
    , Request_(request)
    , PrevRandomSeed_(Parent_->GetPrevRandomSeed())
    , SequenceNumber_(Parent_->GetSequenceNumber())
    , StateHash_(Parent_->GetStateHash())
    , Term_(Parent_->GetTerm())
{ }

TMutationContext::TMutationContext(
    TVersion version,
    const TMutationRequest* request,
    TInstant timestamp,
    ui64 randomSeed,
    ui64 prevRandomSeed,
    i64 sequenceNumber,
    ui64 stateHash,
    int term)
    : THydraContext(
        version,
        timestamp,
        randomSeed)
    , Parent_(nullptr)
    , Request_(request)
    , PrevRandomSeed_(prevRandomSeed)
    , SequenceNumber_(sequenceNumber)
    , StateHash_(stateHash)
    , Term_(term)
{ }

TMutationContext::TMutationContext(TTestingTag)
    : THydraContext(
        TVersion(),
        /*timestamp*/ TInstant::Zero(),
        /*randomSeed*/ 0)
    , Parent_(nullptr)
    , Request_(nullptr)
    , PrevRandomSeed_(0)
    , SequenceNumber_(0)
    , StateHash_(0)
    , Term_(0)
{ }

const TMutationRequest& TMutationContext::Request() const
{
    return *Request_;
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

int TMutationContext::GetTerm() const
{
    return Term_;
}

void TMutationContext::SetStateHash(ui64 newStateHash)
{
    StateHash_ = newStateHash;
}

void TMutationContext::SetResponseData(TSharedRefArray data)
{
    ResponseData_ = std::move(data);
}

void TMutationContext::SetResponseData(TError error)
{
    SetResponseData(CreateErrorResponseMessage(std::move(error)));
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

static NConcurrency::TFlsSlot<TMutationContext*> CurrentMutationContextSlot;

TMutationContext* TryGetCurrentMutationContext()
{
    return *CurrentMutationContextSlot;
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
    *CurrentMutationContextSlot = context;
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
