#pragma once

#include "common.h"

#include "../bus/message.h"

namespace NYT {
namespace NRpc {

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
    const TSharedRef& body,
    const yvector<TSharedRef>& attachments);

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
