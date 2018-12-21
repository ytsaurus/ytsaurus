#pragma once

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/client/api/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulReaderPtr CreateSchemafulTableReader(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    const NTableClient::TTableSchema& schema,
    const NApi::TTableReaderOptions& options,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
