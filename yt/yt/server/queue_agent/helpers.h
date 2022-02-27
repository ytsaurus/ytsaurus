#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(const TQueueTableRow& row);

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr AssertNativeClient(const NApi::IClientPtr& client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
