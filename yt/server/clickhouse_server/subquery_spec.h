#pragma once

#include "private.h"

#include "table_schema.h"

#include <yt/server/clickhouse_server/protos/subquery_spec.pb.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/yson/public.h>
#include <yt/core/ytree/public.h>

#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TSubquerySpec
{
public:
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
    std::vector<NChunkClient::TDataSliceDescriptor> DataSliceDescriptors;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    TQueryId InitialQueryId;
    DB::NamesAndTypesList Columns;
    NTableClient::TTableSchema ReadSchema;

    void Validate() const;

private:
    const std::vector<NChunkClient::TDataSource>& DataSources() const
    {
        return DataSourceDirectory->DataSources();
    }
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec);
void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
