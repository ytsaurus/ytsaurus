#pragma once

#include "private.h"
#include "engine.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerProxy
    : public TRefCounted
{
public:
    TQueryTrackerProxy(
        NApi::IClientPtr stateClient,
        NYPath::TYPath stateRoot,
        TQueryTrackerProxyConfigPtr config,
        int expectedTablesVersion);

    void Reconfigure(const TQueryTrackerProxyConfigPtr& config, const TDuration notIndexedQueriesTTL);

    void StartQuery(
        const TQueryId queryId,
        const EQueryEngine engine,
        const TString& query,
        const NApi::TStartQueryOptions& options,
        const std::string& user);

    void AbortQuery(
        const TQueryId queryId,
        const NApi::TAbortQueryOptions& options,
        const std::string& user);

    NApi::TQueryResult GetQueryResult(
        const TQueryId queryId,
        const i64 resultIndex,
        const std::string& user);

    NApi::IUnversionedRowsetPtr ReadQueryResult(
        const TQueryId queryId,
        const i64 resultIndex,
        const NApi::TReadQueryResultOptions& options,
        const std::string& user);

    NApi::TQuery GetQuery(
        const TQueryId queryId,
        const NApi::TGetQueryOptions& options,
        const std::string& user);

    NApi::TListQueriesResult ListQueries(
        const NApi::TListQueriesOptions& options,
        const std::string& user);

    void AlterQuery(
        const TQueryId queryId,
        const NApi::TAlterQueryOptions& options,
        const std::string& user);

    NApi::TGetQueryTrackerInfoResult GetQueryTrackerInfo(
        const NApi::TGetQueryTrackerInfoOptions& options);

    NApi::TGetQueryDeclaredParametersInfoResult GetQueryDeclaredParametersInfo(
        const NApi::TGetQueryDeclaredParametersInfoOptions& options);

private:
    const NApi::IClientPtr StateClient_;
    const NYPath::TYPath StateRoot_;
    TQueryTrackerProxyConfigPtr ProxyConfig_;
    const int ExpectedTablesVersion_;
    std::unordered_map<EQueryEngine, IProxyEngineProviderPtr> EngineProviders_;
    ISearchIndexPtr TimeBasedIndex_;
    ISearchIndexPtr TokenBasedIndex_;
    TDuration NotIndexedQueriesTTL_;
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerProxy)

////////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxyPtr CreateQueryTrackerProxy(
    NApi::IClientPtr stateClient,
    NYPath::TYPath stateRoot,
    TQueryTrackerProxyConfigPtr config,
    int expectedTablesVersion);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
