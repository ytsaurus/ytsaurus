#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/actions/future.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TLocateSkynetShareOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
