#pragma once

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerProxy
    : public TRefCounted
{
public:
    TQueryTrackerProxy(
        NApi::NNative::IClientPtr stateClient,
        NYPath::TYPath stateRoot,
        TQueryTrackerProxyConfigPtr config);

    void Reconfigure(const TQueryTrackerProxyConfigPtr& config);

    void StartQuery(
        const TQueryId queryId,
        const EQueryEngine engine,
        const TString& query,
        const NApi::TStartQueryOptions& options,
        const TString& user);

    void AbortQuery(
        const TQueryId queryId,
        const NApi::TAbortQueryOptions& options,
        const TString& user);

    NApi::TQueryResult GetQueryResult(
        const TQueryId queryId,
        const i64 resultIndex,
        const TString& user);

    NApi::IUnversionedRowsetPtr ReadQueryResult(
        const TQueryId queryId,
        const i64 resultIndex,
        const NApi::TReadQueryResultOptions& options,
        const TString& user);

    NApi::TQuery GetQuery(
        const TQueryId queryId,
        const NApi::TGetQueryOptions& options,
        const TString& user);

    NApi::TListQueriesResult ListQueries(
        const NApi::TListQueriesOptions& options,
        const TString& user);

    void AlterQuery(
        const TQueryId queryId,
        const NApi::TAlterQueryOptions& options,
        const TString& user);

    NApi::TGetQueryTrackerInfoResult GetQueryTrackerInfo(
        const NApi::TGetQueryTrackerInfoOptions& options);

private:
    const NApi::NNative::IClientPtr StateClient_;
    const NYPath::TYPath StateRoot_;
    TQueryTrackerProxyConfigPtr ProxyConfig_;
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerProxy)

///////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxyPtr CreateQueryTrackerProxy(
    NApi::NNative::IClientPtr stateClient,
    NYPath::TYPath stateRoot,
    TQueryTrackerProxyConfigPtr config);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
