#pragma once

#include "private.h"

#include "conversion.h"

#include <yt/chyt/server/protos/subquery_spec.pb.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/public.h>
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
    std::vector<TSecondaryQueryReadDescriptors> DataSliceDescriptors;
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

class TSecondaryQueryReadTask
{
public:
    std::vector<TSecondaryQueryReadDescriptors> OperandInputs;
};

void ToProto(NProto::TSecondaryQueryReadTask* protoTask, const TSecondaryQueryReadTask& task);
void FromProto(TSecondaryQueryReadTask* task, const NProto::TSecondaryQueryReadTask& protoTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
