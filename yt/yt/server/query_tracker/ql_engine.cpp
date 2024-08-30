#include "ql_engine.h"

#include "config.h"
#include "handler_base.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NApi;
using namespace NYPath;
using namespace NHiveClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TQLQueryHandler
    : public TQueryHandlerBase
{
public:
    TQLQueryHandler(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TEngineConfigBasePtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery,
        const NApi::IClientPtr& queryClient,
        const IInvokerPtr& controlInvoker,
        const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery, stateTimeProfilingCountersMap)
        , Query_(activeQuery.Query)
        , QueryClient_(queryClient)
    { }

    void Start() override
    {
        YT_LOG_DEBUG("Starting QL query");
        OnQueryStarted();
        AsyncQueryResult_ = QueryClient_->SelectRows(Query_);
        AsyncQueryResult_.Subscribe(BIND(&TQLQueryHandler::OnQueryFinish, MakeWeak(this)).Via(GetCurrentInvoker()));
    }

    void Abort() override
    {
        // Nothing smarter than that for now.
        AsyncQueryResult_.Cancel(TError("Query aborted"));
    }

    void Detach() override
    {
        // Nothing smarter than that for now.
        AsyncQueryResult_.Cancel(TError("Query detached"));
    }

private:
    TString Query_;
    NApi::IClientPtr QueryClient_;

    TFuture<TSelectRowsResult> AsyncQueryResult_;

    void OnQueryFinish(const TErrorOr<TSelectRowsResult>& queryResultOrError)
    {
        if (queryResultOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }
        if (!queryResultOrError.IsOK()) {
            OnQueryFailed(queryResultOrError);
            return;
        }
        OnQueryCompleted({TRowset{.Rowset = queryResultOrError.Value().Rowset}});
    }
};

class TQLEngine
    : public IQueryEngine
{
public:
    TQLEngine(IClientPtr stateClient, TYPath stateRoot, const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , ControlQueue_(New<TActionQueue>("QLEngineControl"))
        , StateTimeProfilingCountersMap_(std::move(stateTimeProfilingCountersMap))
        , ClusterDirectory_(DynamicPointerCast<NNative::IConnection>(StateClient_->GetConnection())->GetClusterDirectory())
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        auto settings = ConvertToAttributes(activeQuery.Settings);
        auto cluster = settings->Find<TString>("cluster").value_or(Config_->DefaultCluster);
        auto queryClient = ClusterDirectory_->GetConnectionOrThrow(cluster)->CreateClient(TClientOptions{.User = activeQuery.User});
        return New<TQLQueryHandler>(StateClient_, StateRoot_, Config_, activeQuery, queryClient, ControlQueue_->GetInvoker(), StateTimeProfilingCountersMap_);
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        Config_ = DynamicPointerCast<TQLEngineConfig>(config);
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const TActionQueuePtr ControlQueue_;
    const TStateTimeProfilingCountersMapPtr StateTimeProfilingCountersMap_;
    TQLEngineConfigPtr Config_;
    TClusterDirectoryPtr ClusterDirectory_;
};

IQueryEnginePtr CreateQLEngine(const IClientPtr& stateClient, const TYPath& stateRoot, const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap)
{
    return New<TQLEngine>(stateClient, stateRoot, stateTimeProfilingCountersMap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
