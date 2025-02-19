#include <yt/yt/orm/library/query/continuation.h>

#include <yt/yt/orm/client/misc/public.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <google/protobuf/message.h>

namespace NYT::NOrm::NLibrary::NQuery {

////////////////////////////////////////////////////////////////////////////////

std::string SerializeContinuationToken(
    const google::protobuf::Message& message)
{
    return Base64Encode(message.SerializeAsString());
}

void DeserializeContinuationToken(
    const std::string& token,
    google::protobuf::Message* message)
{
    try {
        if (!message->ParseFromString(Base64StrictDecode(token))) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
                "Error parsing decoded continuation token");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
            "Error deserializing continuation token")
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NLibrary::NQuery
