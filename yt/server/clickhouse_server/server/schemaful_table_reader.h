#pragma once

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/client/api/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulReaderPtr CreateSchemafulTableReader(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    const NTableClient::TTableSchema& schema,
    const NApi::TTableReaderOptions& options,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter());

} // namespace NClickHouse
} // namespace NYT
