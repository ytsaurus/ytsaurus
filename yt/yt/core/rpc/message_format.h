#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>

#include <yt/core/yson/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TSharedRef ConvertMessageToFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const NYson::TProtobufMessageType* messageType,
    const NYson::TYsonString& formatOptionsYson);

TSharedRef ConvertMessageFromFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const NYson::TProtobufMessageType* messageType,
    const NYson::TYsonString& formatOptionsYson);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
