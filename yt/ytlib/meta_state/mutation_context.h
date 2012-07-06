#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>

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

    template <class T>
    T GenerateRandom();

private:
    Stroka MutationType;
    TSharedRef MutationData;
    TInstant Timestamp;
    // TODO: place random generator here

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
