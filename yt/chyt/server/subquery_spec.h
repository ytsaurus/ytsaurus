#pragma once

#include "private.h"

#include "conversion.h"

#include <yt/chyt/server/protos/subquery_spec.pb.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/public.h>
#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_pools/public.h>

#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void FillDataSliceDescriptors(
    TSecondaryQueryReadDescriptors& dataSliceDescriptors,
    const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap,
    const NChunkPools::TChunkStripePtr& chunkStripes);

void FillDataSliceDescriptors(
    std::vector<TSecondaryQueryReadDescriptors>& dataSliceDescriptors,
    const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap,
    const TRange<NChunkPools::TChunkStripePtr>& chunkStripes);

////////////////////////////////////////////////////////////////////////////////

struct TSubquerySpec
{
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
    i64 InputStreamCount;
    std::vector<TSecondaryQueryReadDescriptors> InputSpecs;
    bool InputSpecsTruncated;
    TString InitialQuery;
    // Does not include virtual columns.
    NTableClient::TTableSchemaPtr ReadSchema;
    std::vector<NYTree::IAttributeDictionaryPtr> ColumnAttributes;
    int SubqueryIndex;
    int TableIndex;
    NTableClient::TTableReaderConfigPtr TableReaderConfig;
    TQuerySettingsPtr QuerySettings;
    std::optional<NTableClient::TColumnarStatistics> TableStatistics;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec);
void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec);

////////////////////////////////////////////////////////////////////////////////

struct TSecondaryQueryReadTask
{
    std::vector<TSecondaryQueryReadDescriptors> OperandInputs;
};

void ToProto(NProto::TSecondaryQueryReadTask* protoTask, const TSecondaryQueryReadTask& task);
void FromProto(TSecondaryQueryReadTask* task, const NProto::TSecondaryQueryReadTask& protoTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
