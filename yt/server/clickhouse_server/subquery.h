#pragma once

#include "private.h"
#include "query_analyzer.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>
#include <yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/logging/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TSubquery
{
    NChunkPools::TChunkStripeListPtr StripeList;
    NChunkPools::IChunkPoolOutput::TCookie Cookie;
    std::pair<NTableClient::TUnversionedOwningRow, NTableClient::TUnversionedOwningRow> Limits;
};

//! Fetch data slices for given input tables and fill given subquery spec template.
NChunkPools::TChunkStripeListPtr FetchInput(
    TBootstrap* bootstrap,
    NApi::NNative::IClientPtr client,
    const IInvokerPtr& invoker,
    std::vector<NTableClient::TTableSchema> tableSchemas,
    std::vector<std::vector<NYPath::TRichYPath>> inputTablePaths,
    std::vector<std::optional<DB::KeyCondition>> keyConditions,
    NTableClient::TRowBufferPtr rowBuffer,
    TSubqueryConfigPtr config,
    TSubquerySpec& specTemplate);

std::vector<TSubquery> BuildSubqueries(
    const NChunkPools::TChunkStripeListPtr& inputStripeList,
    std::optional<int> keyColumnCount,
    EPoolKind poolKind,
    int jobCount,
    std::optional<double> samplingRate,
    const DB::Context& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
