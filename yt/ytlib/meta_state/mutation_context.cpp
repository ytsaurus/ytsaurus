#include "stdafx.h"
#include "mutation_context.h"

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    const TMetaVersion& version,
    const TMutationRequest& request,
    TInstant timestamp,
    ui64 randomSeed)
    : Version(version)
    , Request(request)
    , Timestamp(timestamp)
    , RandomGenerator_(randomSeed)
{ }

const TMetaVersion& TMutationContext::GetVersion() const
{
    return Version;
}

const Stroka& TMutationContext::GetType() const
{
    return Request.Type;
}

TRef TMutationContext::GetRequestData() const
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
    return RandomGenerator_;
}

TSharedRef TMutationContext::GetResponseData() const
{
    return ResponseData;
}

void TMutationContext::SetResponseData(const TSharedRef& data)
{
    ResponseData = data;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
