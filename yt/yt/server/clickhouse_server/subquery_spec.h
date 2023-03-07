#pragma once

#include "private.h"

#include "schema.h"

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
    NTableClient::TTableSchema ReadSchema;
    // TODO(max42): CHYT-154.
    NYson::TYsonString MembershipHint;
    int SubqueryIndex;
    int TableIndex;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec);
void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
