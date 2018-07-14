#pragma once

#include "public.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/ypath/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<NTableClient::ISchemalessWriterPtr> CreateTableWriter(
    const IClientPtr& client,
    const NYPath::TRichYPath& path,
    const TTableWriterOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
