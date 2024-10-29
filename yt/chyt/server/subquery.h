#pragma once

#include "private.h"
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
};

//! Fetch data slices for given input tables.
TQueryInput FetchInput(
    TStorageContext* storageContext,
    const TQueryAnalysisResult& queryAnalysisResult,
    const std::vector<std::string>& realColumnNames,
    const std::vector<std::string>& virtualColumnNames,
    const TClickHouseIndexBuilder& indexBuilder,
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
