#include "continuation_token.h"

#include <yp/client/api/misc/public.h>

#include <yt/core/misc/error.h>

#include <contrib/libs/protobuf/message.h>

#include <library/string_utils/base64/base64.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TString SerializeContinuationToken(
    const google::protobuf::Message& message)
{
    return Base64Encode(message.SerializeAsString());
}

void DeserializeContinuationToken(
    const TString& token,
    google::protobuf::Message* message)
{
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

} // namespace NYP::NServer::NObjects
