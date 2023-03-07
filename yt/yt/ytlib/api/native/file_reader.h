#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<IFileReaderPtr> CreateFileReader(
    IClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
