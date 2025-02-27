#pragma once

#include <util/generic/fwd.h>

#include <google/protobuf/message.h>

namespace NYT::NOrm::NLibrary::NQuery {

////////////////////////////////////////////////////////////////////////////////

std::string SerializeContinuationToken(
    const google::protobuf::Message& message);

void DeserializeContinuationToken(
    const std::string& token,
    google::protobuf::Message* message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NLibrary::NQuery
