#include "stdafx.h"
#include "mutation_context.h"

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

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
