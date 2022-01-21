#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

bool TryParseObjectId(
    const NYPath::TYPath& path,
    NObjectClient::TObjectId* objectId,
    NYPath::TYPath* pathSuffix = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
