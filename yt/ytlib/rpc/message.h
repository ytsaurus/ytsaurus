#pragma once

#include "common.h"

#include "../bus/message.h"

#include <contrib/libs/protobuf/message.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(const google::protobuf::Message* message, TBlob* data);
bool DeserializeMessage(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

NBus::IMessage::TPtr CreateRequestMessage(
    const TRequestId& requestId,
    const Stroka& path,
    const Stroka& verb,
    TBlob&& body,
    const yvector<TSharedRef>& attachments);

NBus::IMessage::TPtr CreateResponseMessage(
    const TRequestId& requestId,
    const TError& error,
    TBlob&& body,
    const yvector<TSharedRef>& attachments);

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
