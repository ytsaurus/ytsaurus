#pragma once

#include "public.h"

#include <core/misc/ref.h>
#include <core/misc/random.h>

#include <core/actions/callback.h>

#include <ytlib/hydra/version.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TMutationRequest
{
    TMutationRequest();
    TMutationRequest(
        Stroka type,
        TSharedRef data,
        TCallback<void(TMutationContext*)> action = TCallback<void(TMutationContext*)>());

    Stroka Type;
    TSharedRef Data;
    TCallback<void(TMutationContext*)> Action;
    bool AllowLeaderForwarding = false;
};

struct TMutationResponse
{
    TMutationResponse();
    explicit TMutationResponse(TSharedRefArray data);

    TSharedRefArray Data;
};

class TMutationContext
{
public:
    TMutationContext(
        TMutationContext* parent,
        const TMutationRequest& request);

    TMutationContext(
        TVersion version,
        const TMutationRequest& request,
        TInstant timestamp,
        ui64 randomSeed);

    TVersion GetVersion() const;
    const TMutationRequest& Request() const;
    TInstant GetTimestamp() const;
    TRandomGenerator& RandomGenerator();

    TMutationResponse& Response();

private:
    TMutationContext* Parent_;
    TVersion Version_;
    const TMutationRequest& Request_;
    TMutationResponse Response_;
    TInstant Timestamp_;
    TRandomGenerator RandomGenerator_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

