#pragma once

#include "public.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TString SerializeContinuationToken(
    const google::protobuf::Message& message);

void DeserializeContinuationToken(
    const TString& token,
    google::protobuf::Message* message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
