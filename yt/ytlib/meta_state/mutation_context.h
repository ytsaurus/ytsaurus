#pragma once

#include "public.h"
#include "meta_version.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/random.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMutationContext
{
public:
    TMutationContext(
        const TMetaVersion& version,
        const Stroka& mutationType,
        const TSharedRef& mutationData,
        TInstant timestamp,
        ui64 randomSeed);

    const TMetaVersion& GetVersion() const;
    const Stroka& GetMutationType() const;
    TSharedRef GetMutationData() const;
    TInstant GetTimestamp() const;

    TRandomGenerator& RandomGenerator();

private:
    TMetaVersion Version;
    Stroka MutationType;
    TSharedRef MutationData;
    TInstant Timestamp;
    TRandomGenerator RandomGenerator_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

