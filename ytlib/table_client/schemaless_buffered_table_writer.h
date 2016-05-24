#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/ypath/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NApi::IClientPtr client,
    TNameTablePtr nameTable,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
