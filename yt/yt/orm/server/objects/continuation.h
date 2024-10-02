#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TString SerializeContinuationToken(
    const google::protobuf::Message& message);

void DeserializeContinuationToken(
    const TString& token,
    google::protobuf::Message* message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
