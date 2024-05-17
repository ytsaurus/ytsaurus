#pragma once

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NYPath;
using namespace NYson;

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerProxy
    : public TRefCounted
{
public:
    TQueryTrackerProxy(
        NNative::IClientPtr stateClient,
        TYPath stateRoot);

    void StartQuery(
        const TQueryId queryId,
        const NQueryTrackerClient::EQueryEngine engine,
        const TString& query,
        const TStartQueryOptions& options,
        const TString& user);

    void AbortQuery(
        const TQueryId queryId,
        const TAbortQueryOptions& options,
        const TString& user);

    TQueryResult GetQueryResult(
        const TQueryId queryId,
        const i64 resultIndex,
        const TString& user);

    IUnversionedRowsetPtr ReadQueryResult(
        const TQueryId queryId,
        const i64 resultIndex,
        const TReadQueryResultOptions& options,
        const TString& user);

    TQuery GetQuery(
        const TQueryId queryId,
        const TGetQueryOptions& options,
        const TString& user);

    TListQueriesResult ListQueries(
        const TListQueriesOptions& options,
        const TString& user);

    void AlterQuery(
        const TQueryId queryId,
        const TAlterQueryOptions& options,
        const TString& user);

private:
    const NNative::IClientPtr StateClient_;
    const TYPath StateRoot_;
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerProxy)

///////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxyPtr CreateQueryTrackerProxy(
    NNative::IClientPtr stateClient,
    NYPath::TYPath stateRoot
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
