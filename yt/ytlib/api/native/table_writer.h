#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/client/ypath/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableWriterPtr> CreateTableWriter(
    const IClientPtr& client,
    const NYPath::TRichYPath& path,
    const TTableWriterOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
