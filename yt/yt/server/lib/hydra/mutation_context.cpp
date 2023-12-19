#include "mutation_context.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/message.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TString MockHostSanitize(TStringBuf)
{
    return "";
}

////////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    TMutationContext* parent,
    const TMutationRequest* request)
    : THydraContext(
        parent->GetVersion(),
        parent->GetTimestamp(),
        parent->GetRandomSeed(),
        parent->RandomGenerator(),
        parent->HostNameSanitizer_)
    , Parent_(parent)
    , Request_(request)
    , HostNameSanitizer_(parent->HostNameSanitizer_)
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
    int term,
    TErrorSanitizerGuard::THostNameSanitizer hostNameSanitizer)
    : THydraContext(
        version,
        timestamp,
        randomSeed,
        hostNameSanitizer)
    , Parent_(nullptr)
    , Request_(request)
    , HostNameSanitizer_(hostNameSanitizer)
    , PrevRandomSeed_(prevRandomSeed)
    , SequenceNumber_(sequenceNumber)
    , StateHash_(stateHash)
    , Term_(term)
{ }

TMutationContext::TMutationContext(TTestingTag)
    : THydraContext(
        TVersion(),
        /*timestamp*/ TInstant::Zero(),
        /*randomSeed*/ 0,
        BIND(&MockHostSanitize))
    , Parent_(nullptr)
    , Request_(nullptr)
    , HostNameSanitizer_({})
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

TSharedRefArray TMutationContext::TakeResponseData()
{
    return std::move(ResponseData_);
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

YT_THREAD_LOCAL(TMutationContext*) CurrentMutationContextSlot;

TMutationContext* TryGetCurrentMutationContext()
{
    return CurrentMutationContextSlot;
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
    CurrentMutationContextSlot = context;
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
