#pragma once

#include "public.h"
#include "client.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/ypath/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateFileReader(
    INativeClientPtr client,
    const NYPath::TYPath& path,
    const TFileReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

