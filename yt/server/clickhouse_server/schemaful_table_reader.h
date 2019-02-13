#pragma once

#include <yt/client/table_client/public.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/client/api/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulReaderPtr CreateSchemafulTableReader(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    const NTableClient::TTableSchema& schema,
    const NApi::TTableReaderOptions& options,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
