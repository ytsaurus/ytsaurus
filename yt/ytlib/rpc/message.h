#pragma once

#include "public.h"
#include "error.h"

#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/bus/message.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

NBus::IMessage::TPtr CreateRequestMessage(
    const NProto::TRequestHeader& header,
    TBlob&& body,
    const yvector<TSharedRef>& attachments);

NBus::IMessage::TPtr CreateResponseMessage(
    const NProto::TResponseHeader& header,
    const TSharedRef& body,
    const yvector<TSharedRef>& attachments);

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const NProto::TResponseHeader& header);

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error);

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TError& error);

NProto::TRequestHeader GetRequestHeader(NBus::IMessage* message);
NBus::IMessage::TPtr SetRequestHeader(
    NBus::IMessage* message,
    const NProto::TRequestHeader& header);

NProto::TResponseHeader GetResponseHeader(NBus::IMessage* message);
NBus::IMessage::TPtr SetResponseHeader(
    NBus::IMessage* message,
    const NProto::TResponseHeader& header);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
