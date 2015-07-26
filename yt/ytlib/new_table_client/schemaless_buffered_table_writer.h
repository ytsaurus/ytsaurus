#pragma once

#include "public.h"

#include <ytlib/transaction_client/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    NChunkClient::TRemoteWriterOptionsPtr options,
    NApi::IClientPtr client,
    TNameTablePtr nameTable,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
