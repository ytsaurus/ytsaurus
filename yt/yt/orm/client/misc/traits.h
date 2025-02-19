#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt_proto/yt/orm/client/proto/object.pb.h>

namespace NYT::NOrm::NClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReferenceCardinality,
    (Single)
    (Multi)
);

struct TReferenceDescriptor
{
    EReferenceCardinality Cardinality;
    NYT::NOrm::NClient::NProto::TReferenceOption Option;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient
