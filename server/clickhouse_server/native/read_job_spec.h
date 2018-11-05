#pragma once

#include "public.h"

#include "table_schema.h"

#include <yt/server/clickhouse_server/native/protos/read_job_spec.pb.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/yson/public.h>
#include <yt/core/ytree/public.h>

#include <vector>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

class TReadJobSpec
{
public:
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
    std::vector<NChunkClient::TDataSliceDescriptor> DataSliceDescriptors;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;

public:
    void Validate() const;

    NChunkClient::EDataSourceType GetCommonDataSourceType() const;
    NTableClient::TTableSchema GetCommonNativeSchema() const;
    TTableList GetTables() const;

private:
    const std::vector<NChunkClient::TDataSource>& DataSources() const
    {
        return DataSourceDirectory->DataSources();
    }
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TReadJobSpec* protoSpec, const TReadJobSpec& spec);
void FromProto(TReadJobSpec* spec, const NProto::TReadJobSpec& protoSpec);

void Serialize(const TReadJobSpec& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TReadJobSpec& spec, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
