#include "stdafx.h"
#include "mutation_context.h"

#include <core/concurrency/fls.h>

namespace NYT {
namespace NHydra {

///////////////////////////////////////////////////////////////////////////////

TMutationRequest::TMutationRequest()
{ }

TMutationRequest::TMutationRequest(
    Stroka type,
    TSharedRef data,
    TCallback<void(TMutationContext*)> action)
    : Type(std::move(type))
    , Data(std::move(data))
    , Action(std::move(action))
{ }

///////////////////////////////////////////////////////////////////////////////

TMutationResponse::TMutationResponse()
{ }

TMutationResponse::TMutationResponse(TSharedRefArray data)
    : Data(std::move(data))
{ }

///////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    TMutationContext* parent,
    const TMutationRequest& request)
    : Parent_(parent)
    , Version_(Parent_->GetVersion())
    , Request_(request)
    , Timestamp_(Parent_->GetTimestamp())
{ }

TMutationContext::TMutationContext(
    TVersion version,
    const TMutationRequest& request,
    TInstant timestamp,
    ui64 randomSeed)
    : Parent_(nullptr)
    , Version_(version)
    , Request_(request)
    , Timestamp_(timestamp)
    , RandomGenerator_(randomSeed)
{ }

TVersion TMutationContext::GetVersion() const
{
    return Version_;
}

const TMutationRequest& TMutationContext::Request() const
{
    return Request_;
}

TInstant TMutationContext::GetTimestamp() const
{
    return Timestamp_;
}

TRandomGenerator& TMutationContext::RandomGenerator()
{
    return Parent_ ? Parent_->RandomGenerator() : RandomGenerator_;
}

TMutationResponse& TMutationContext::Response()
{
    return Response_;
}

static NConcurrency::TFls<TMutationContext*> CurrentMutationContext;

TMutationContext* TryGetCurrentMutationContext()
{
    return *CurrentMutationContext;
}

TMutationContext* GetCurrentMutationContext()
{
    auto* context = TryGetCurrentMutationContext();
    YASSERT(context);
    return context;
}

bool HasMutationContext()
{
    return TryGetCurrentMutationContext() != nullptr;
}

void SetCurrentMutationContext(TMutationContext* context)
{
    *CurrentMutationContext = context;
}

///////////////////////////////////////////////////////////////////////////////

TMutationContextGuard::TMutationContextGuard(TMutationContext* context)
    : Context_(context)
    , SavedContext_(TryGetCurrentMutationContext())
{
    YASSERT(Context_);
    SetCurrentMutationContext(Context_);
}

TMutationContextGuard::~TMutationContextGuard()
{
    YASSERT(GetCurrentMutationContext() == Context_);
    SetCurrentMutationContext(SavedContext_);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
