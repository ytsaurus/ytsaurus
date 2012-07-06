#include "stdafx.h"
#include "mutation_context.h"

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

TMutationContext::TMutationContext(
    const Stroka& mutationType,
    const TSharedRef& mutationData,
    TInstant timestamp,
    ui64 randomSeed)
    : MutationType(mutationType)
    , MutationData(mutationData)
    , Timestamp(timestamp)
{ }

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

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
