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
    TClosure action /*= TClosure()*/ )
    : Type(std::move(type))
    , Data(std::move(data))
    , Action(std::move(action))
{ }

///////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    TMutationContext* parent,
    const TMutationRequest& request)
    : Parent(parent)
    , Version(Parent->GetVersion())
    , Request(request)
    , Timestamp(Parent->GetTimestamp())
    , MutationSuppressed(false)
{ }

TMutationContext::TMutationContext(
    TVersion version,
    const TMutationRequest& request,
    TInstant timestamp,
    ui64 randomSeed)
    : Parent(nullptr)
    , Version(version)
    , Request(request)
    , Timestamp(timestamp)
    , RandomGenerator_(randomSeed)
    , MutationSuppressed(false)
{ }

TVersion TMutationContext::GetVersion() const
{
    return Version;
}

const Stroka& TMutationContext::GetType() const
{
    return Request.Type;
}

const TRef& TMutationContext::GetRequestData() const
{
    return Request.Data;
}

const TClosure& TMutationContext::GetRequestAction() const
{
    return Request.Action;
}

const TMutationId& TMutationContext::GetId() const
{
    return Request.Id;
}

TInstant TMutationContext::GetTimestamp() const
{
    return Timestamp;
}

TRandomGenerator& TMutationContext::RandomGenerator()
{
    return Parent ? Parent->RandomGenerator() : RandomGenerator_;
}

TSharedRef TMutationContext::GetResponseData() const
{
    return ResponseData;
}

void TMutationContext::SetResponseData(TSharedRef data)
{
    ResponseData = std::move(data);
}

void TMutationContext::SuppressMutation()
{
    MutationSuppressed = true;
}

bool TMutationContext::IsMutationSuppressed() const
{
    return MutationSuppressed;
}

///////////////////////////////////////////////////////////////////////////////


} // namespace NHydra
} // namespace NYT
