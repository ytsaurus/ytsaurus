#include "stdafx.h"
#include "mutation_context.h"

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    const TMetaVersion& version,
    const Stroka& mutationType,
    const TSharedRef& mutationData,
    TInstant timestamp,
    ui64 randomSeed)
    : Version(version)
    , MutationType(mutationType)
    , MutationData(mutationData)
    , Timestamp(timestamp)
    , RandomGenerator_(randomSeed)
{ }

const TMetaVersion& TMutationContext::GetVersion() const
{
    return Version;
}

const Stroka& TMutationContext::GetMutationType() const
{
    return MutationType;
}

TSharedRef TMutationContext::GetMutationData() const
{
    return MutationData;
}

TInstant TMutationContext::GetTimestamp() const
{
    return Timestamp;
}

TRandomGenerator& TMutationContext::RandomGenerator()
{
    return RandomGenerator_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
