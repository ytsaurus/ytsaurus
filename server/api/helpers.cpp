#include "helpers.h"
#include "private.h"

#include <library/string_utils/base64/base64.h>

#include <contrib/libs/protobuf/message.h>

namespace NYP::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

TString SerializeContinuationToken(const google::protobuf::Message& message)
{
    return Base64Encode(message.SerializeAsString());
}

void DeserializeContinuationToken(const TString& token, google::protobuf::Message* message)
{
    // TODO(avitella): Do it without exceptions in Base64Decode.
    try {
        if (!message->ParseFromString(Base64StrictDecode(token))) {
            THROW_ERROR_EXCEPTION(
                NYP::NClient::NApi::EErrorCode::InvalidContinuationToken,
                "Error parsing decoded continuation token");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            NYP::NClient::NApi::EErrorCode::InvalidContinuationToken,
            "Error deserializing continuation token")
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NApi

