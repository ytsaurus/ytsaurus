#pragma once

#include "public.h"

#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateRequestMessage(
    const NProto::TRequestHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments);

TSharedRefArray CreateResponseMessage(
    const NProto::TResponseHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments);

TSharedRefArray CreateResponseMessage(
    const ::google::protobuf::MessageLite& body,
    const std::vector<TSharedRef>& attachments = std::vector<TSharedRef>());

TSharedRefArray CreateErrorResponseMessage(
    const NProto::TResponseHeader& header);

TSharedRefArray CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error);

TSharedRefArray CreateErrorResponseMessage(
    const TError& error);

bool ParseRequestHeader(
    TSharedRefArray message,
    NProto::TRequestHeader* header);

TSharedRefArray SetRequestHeader(
    TSharedRefArray message,
    const NProto::TRequestHeader& header);

bool ParseResponseHeader(
    TSharedRefArray message,
    NProto::TResponseHeader* header);

TSharedRefArray SetResponseHeader(
    TSharedRefArray message,
    const NProto::TResponseHeader& header);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
