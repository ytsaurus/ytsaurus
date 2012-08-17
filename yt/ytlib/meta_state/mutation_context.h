#pragma once

#include "public.h"
#include "meta_version.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/random.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct TMutationRequest
{
    TMutationRequest()
    { }

    TMutationRequest(
        const Stroka& type,
        const TSharedRef& data,
        const TClosure& action = TClosure())
        : Type(type)
        , Data(data)
        , Action(action)
    { }

    Stroka Type;
    TSharedRef Data;
    TClosure Action;
    TMutationId Id;
};

struct TMutationResponse
{
    TMutationResponse()
        : Applied(false)
    { }

    bool Applied;
    TSharedRef Data;
};

class TMutationContext
{
public:
    TMutationContext(
        const TMetaVersion& version,
        const TMutationRequest& request,
        TInstant timestamp,
        ui64 randomSeed);

    const TMetaVersion& GetVersion() const;
    const Stroka& GetType() const;
    TRef GetRequestData() const;
    const TClosure& GetRequestAction() const;
    const TMutationId& GetId() const;
    TInstant GetTimestamp() const;

    TSharedRef GetResponseData() const;
    void SetResponseData(const TSharedRef& data);

    TRandomGenerator& RandomGenerator();

private:
    TMetaVersion Version;
    TMutationRequest Request;
    TInstant Timestamp;
    TRandomGenerator RandomGenerator_;
    TSharedRef ResponseData;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

