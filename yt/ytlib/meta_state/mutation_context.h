#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/random.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMutationContext
{
public:
    TMutationContext(
        const Stroka& mutationType,
        const TSharedRef& mutationData,
        TInstant timestamp,
        ui64 randomSeed);

    const Stroka& GetMutationType() const;
    TSharedRef GetMutationData() const;
    TInstant GetTimestamp() const;

    TRandomGenerator& RandomGenerator();

private:
    Stroka MutationType;
    TSharedRef MutationData;
    TInstant Timestamp;
    TRandomGenerator RandomGenerator_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

