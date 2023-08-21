#pragma once

#include "private.h"
#include "query_analyzer.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TSubquery
{
    NChunkPools::TChunkStripeListPtr StripeList;
    NChunkPools::IChunkPoolOutput::TCookie Cookie;
    std::pair<NTableClient::TOwningKeyBound, NTableClient::TOwningKeyBound> Bounds;
};

struct TQueryInput
{
    NChunkPools::TChunkStripeListPtr StripeList;
    THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr> MiscExtMap;
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory;
};

//! Fetch data slices for given input tables.
TQueryInput FetchInput(
    TStorageContext* storageContext,
    const TQueryAnalysisResult& queryAnalysisResult,
    const std::vector<TString>& realColumnNames,
    const std::vector<TString>& virtualColumnNames,
    const TClickHouseIndexBuilder& indexBuilder,
    NTransactionClient::TTransactionId transactionId);

std::vector<TSubquery> BuildThreadSubqueries(
    const NChunkPools::TChunkStripeListPtr& inputStripeList,
    std::optional<int> keyColumnCount,
    EPoolKind poolKind,
    NChunkClient::TDataSourceDirectoryPtr dataSourceDirectory,
    int jobCount,
    std::optional<double> samplingRate,
    const TStorageContext* storageContext,
    const TSubqueryConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
