#pragma once

#include "private.h"
#include "index_stats.h"
#include "query_analyzer.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/input_stream.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TQueryInput
{
    int OperandCount;
    std::vector<TTablePtr> InputTables;
    NChunkPools::TChunkStripeListPtr StripeList;
    THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr> MiscExtMap;
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
    NChunkPools::TInputStreamDirectory InputStreamDirectory;
    std::vector<std::shared_ptr<IChytIndexStat>> IndexStats;
    std::optional<NTableClient::TColumnarStatistics> TableStatistics;
};

std::optional<NTableClient::TColumnarStatistics> FetchStatistics(
    TStorageContext* storageContext,
    std::vector<NTableClient::TTableSchemaPtr> operandSchemas,
    std::vector<std::vector<TTablePtr>> tables,
    const std::vector<std::string>& realColumnNames,
    NTransactionClient::TTransactionId transactionId);

//! Fetch data slices for given input tables.
TQueryInput FetchInput(
    TStorageContext* storageContext,
    std::vector<NTableClient::TTableSchemaPtr> operandSchemas,
    std::vector<std::vector<TTablePtr>> tables,
    std::vector<std::optional<DB::KeyCondition>> keyConditions,
    std::vector<NChunkClient::TReadRange> keyReadRanges,
    bool needTableStatistics,
    const std::vector<std::string>& realColumnNames,
    const std::vector<std::string>& virtualColumnNames,
    std::optional<TClickHouseIndexBuilder> indexBuilder,
    NTransactionClient::TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

struct TSubquery
{
    NChunkPools::TChunkStripeListPtr StripeList;
    NChunkPools::IChunkPoolOutput::TCookie Cookie;
    std::pair<NTableClient::TOwningKeyBound, NTableClient::TOwningKeyBound> Bounds;
};

std::vector<TSubquery> BuildThreadSubqueries(
    const TQueryInput& queryInput,
    const TQueryAnalysisResult& queryAnalysisResult,
    int jobCount,
    std::optional<double> samplingRate,
    const TStorageContext* storageContext,
    const TSubqueryConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
