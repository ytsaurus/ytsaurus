#pragma once

#include "private.h"

#include "conversion.h"

#include <yt/server/clickhouse_server/protos/subquery_spec.pb.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>

#include <yt/core/yson/public.h>
#include <yt/core/ytree/public.h>

#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TSubquerySpec
{
public:
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
    std::vector<std::vector<NChunkClient::TDataSliceDescriptor>> DataSliceDescriptors;
    TQueryId InitialQueryId;
    TString InitialQuery;
    // Does not include virtual columns.
    NTableClient::TTableSchemaPtr ReadSchema;
    int SubqueryIndex;
    int TableIndex;
    NTableClient::TTableReaderConfigPtr TableReaderConfig;
    TQuerySettingsPtr QuerySettings;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec);
void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
