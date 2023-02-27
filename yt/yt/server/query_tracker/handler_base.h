#pragma once

#include "engine.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NQueryTracker {

///////////////////////////////////////////////////////////////////////////////

class TQueryHandlerBase
    : public IQueryHandler
{
public:
    TQueryHandlerBase(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TEngineConfigBasePtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery);

    //! Starts a transaction and validates that by the moment of its start timestamp,
    //! incarnation of a query is still the same. Context switch happens inside.
    NApi::ITransactionPtr StartIncarnationTransaction() const;

protected:
    const NApi::IClientPtr StateClient_;
    const NYPath::TYPath StateRoot_;
    const TEngineConfigBasePtr Config_;
    const TString Query_;
    const TQueryId QueryId_;
    const i64 Incarnation_;
    const TString User_;
    const EQueryEngine Engine_;
    const NYTree::INodePtr Settings_;

    const NLogging::TLogger Logger;

    NYson::TYsonString Progress_ = NYson::TYsonString(TString("{}"));

    void OnProgress(const NYson::TYsonString& progress);
    void OnQueryFailed(const TError& error);
    void OnQueryCompleted(const std::vector<TErrorOr<NApi::IUnversionedRowsetPtr>>& rowsetOrErrors);
    void OnQueryCompleted(const std::vector<TErrorOr<TSharedRef>>& wireRowsetOrErrors);

    bool TryWriteQueryState(EQueryState state, const TError& error, const std::vector<TErrorOr<TSharedRef>>& wireRowsetOrErrors);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
