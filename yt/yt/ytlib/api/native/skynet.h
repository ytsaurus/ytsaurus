#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
    const IClientPtr& client,
    const NYPath::TRichYPath& path,
    const TLocateSkynetShareOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
