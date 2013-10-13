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
        TClosure action = TClosure());

    Stroka Type;
    TSharedRef Data;
    TClosure Action;
    TMutationId Id;
};

struct TMutationResponse
{
    TSharedRef Data;
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
    const Stroka& GetType() const;
    const TRef& GetRequestData() const;
    const TClosure& GetRequestAction() const;
    const TMutationId& GetId() const;
    TInstant GetTimestamp() const;

    TSharedRef GetResponseData() const;
    void SetResponseData(TSharedRef data);

    TRandomGenerator& RandomGenerator();

private:
    TMutationContext* Parent;
    TVersion Version;
    TMutationRequest Request;
    TInstant Timestamp;
    TRandomGenerator RandomGenerator_;
    TSharedRef ResponseData;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

