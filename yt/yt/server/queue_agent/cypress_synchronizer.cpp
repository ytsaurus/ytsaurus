#include "cypress_synchronizer.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NQueueClient;
using namespace NRpc;
using namespace NTracing;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressSynchronizerLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECypressSyncObjectType,
    (Queue)
    (Consumer)
);

namespace {

////////////////////////////////////////////////////////////////////////////////

TRowRevision NextRowRevision(const std::optional<TRowRevision> rowRevision)
{
    return rowRevision.value_or(0) + 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

struct TObject
{
    //! The location (cluster, path) of the object in question.
    TCrossClusterReference Object;
    ECypressSyncObjectType Type;
    //! The revision of the corresponding Cypress node.
    std::optional<NHydra::TRevision> Revision;
    //! The internal revision of the corresponding dynamic state row.
    std::optional<TRowRevision> RowRevision;
};

////////////////////////////////////////////////////////////////////////////////

class TCypressSynchronizerPassSession final
{
public:
    TCypressSynchronizerPassSession(
        ECypressSynchronizerPolicy policy,
        std::vector<TString> clusters,
        TDynamicStatePtr dynamicState,
        TClientDirectoryPtr clientDirectory,
        NLogging::TLogger logger)
        : Policy_(policy)
        , Clusters_(std::move(clusters))
        , DynamicState_(std::move(dynamicState))
        , ClientDirectory_(std::move(clientDirectory))
        , Logger(logger)
    { }

    std::vector<TError> Build()
    {
        FetchObjectMaps();
        ListObjectChanges();
        DeleteObjects();
        FetchAttributes();
        WriteRows();

        return Alerts_;
    }

private:
    const ECypressSynchronizerPolicy Policy_;
    const std::vector<TString> Clusters_;
    const TDynamicStatePtr DynamicState_;
    const TClientDirectoryPtr ClientDirectory_;
    const NLogging::TLogger Logger;

    using TObjectMap = THashMap<TString, std::vector<TObject>>;

    struct TObjectRowList
    {
        std::vector<TQueueTableRow> QueueRows;
        std::vector<TConsumerTableRow> ConsumerRows;

        // NB: Must provide a strong exception-safety guarantee.
        void AppendObject(const TObject& object, const IAttributeDictionaryPtr& attributes)
        {
            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    QueueRows.push_back(TQueueTableRow::FromAttributeDictionary(
                        object.Object,
                        NextRowRevision(object.RowRevision),
                        attributes));
                    break;
                case ECypressSyncObjectType::Consumer:
                    ConsumerRows.push_back(TConsumerTableRow::FromAttributeDictionary(
                        object.Object,
                        NextRowRevision(object.RowRevision),
                        attributes));
                    break;
            }
        }

        void AppendObjectWithError(const TObject& object, const TError& error)
        {
            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    QueueRows.push_back({
                        .Ref = object.Object,
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .SynchronizationError = error,
                    });
                    break;
                case ECypressSyncObjectType::Consumer:
                    ConsumerRows.push_back({
                        .Ref = object.Object,
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .SynchronizationError = error,
                    });
                    break;
            }
        }

        void AppendObjectKey(const TObject& object)
        {
            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    QueueRows.push_back({.Ref = object.Object});
                    break;
                case ECypressSyncObjectType::Consumer:
                    ConsumerRows.push_back({.Ref = object.Object});
                    break;
            }
        }

        void MergeWith(const TObjectRowList& rhs)
        {
            QueueRows.insert(QueueRows.end(), rhs.QueueRows.begin(), rhs.QueueRows.end());
            ConsumerRows.insert(ConsumerRows.end(), rhs.ConsumerRows.begin(), rhs.ConsumerRows.end());
        }

        bool Empty() const
        {
            return QueueRows.empty() && ConsumerRows.empty();
        }
    };

    // Session state.

    TObjectMap ClusterToDynamicStateObjects_;
    TObjectMap ClusterToModifiedObjects_;
    std::vector<TObject> ObjectsToDelete_;
    TObjectRowList RowsWithErrors_;
    TObjectRowList RowsToDelete_;
    TObjectRowList RowsToWrite_;

    std::vector<TError> Alerts_;

    void ListObjectChanges()
    {
        switch (Policy_) {
            case ECypressSynchronizerPolicy::Polling:
                ListObjectChangesPolling();
                break;
            case ECypressSynchronizerPolicy::Watching:
                ListObjectChangesWatching();
                break;
        }
    }

    //! Fetch revisions for all objects in the dynamic state and return the ones with a new Cypress revision.
    void ListObjectChangesPolling()
    {
        // Fetch Cypress revisions for all objects in dynamic state.

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        std::vector<TString> clusters;
        for (const auto& [cluster, objects] : ClusterToDynamicStateObjects_) {
            auto proxy = CreateObjectServiceReadProxy(
                GetNativeClientOrThrow(cluster),
                EMasterChannelKind::Follower);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& object : objects) {
                batchReq->AddRequest(TYPathProxy::Get(object.Object.Path + "/@attribute_revision"));
            }
            asyncResults.push_back(batchReq->Invoke());
            clusters.push_back(cluster);
        }

        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        // Collect all objects for which the current Cypress revision is larger than the stored revision.

        std::vector<TError> clusterRevisionFetchingAlerts;
        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& batchRsp = combinedResults[index];
            const auto& cluster = clusters[index];
            if (!batchRsp.IsOK()) {
                clusterRevisionFetchingAlerts.push_back(GetCumulativeError(batchRsp) << TErrorAttribute("cluster", cluster));
                continue;
            }
            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToDynamicStateObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        responseOrError,
                        "Error fetching revision for object %v",
                        object.Object);
                    RowsWithErrors_.AppendObjectWithError(object, responseOrError);
                    continue;
                }

                std::optional<NHydra::TRevision> revision;
                try {
                    revision = ConvertTo<NHydra::TRevision>(TYsonString(responseOrError.Value()->value()));
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing revision for object %v",
                        object.Object);
                    RowsWithErrors_.AppendObjectWithError(object, ex);
                    continue;
                }
                if (!object.Revision || *object.Revision < *revision) {
                    YT_LOG_DEBUG(
                        "Object Cypress revision changed (Object: %v, Revision: %x -> %x)",
                        object.Object,
                        object.Revision,
                        revision);
                    ClusterToModifiedObjects_[cluster].push_back(object);
                }
            }
        }

        if (!clusterRevisionFetchingAlerts.empty()) {
            Alerts_.push_back(
                TError(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchObjectRevisions,
                    "Error fetching object revisions from clusters")
                    << clusterRevisionFetchingAlerts);
        }
    }

    struct TCypressWatchlist
        : public TYsonStructLite
    {
        THashMap<TString, NHydra::TRevision> Queues;
        THashMap<TString, NHydra::TRevision> Consumers;

        THashMap<TString, NHydra::TRevision>& ObjectsByType(ECypressSyncObjectType type)
        {
            switch (type) {
                case ECypressSyncObjectType::Queue:
                    return Queues;
                case ECypressSyncObjectType::Consumer:
                    return Consumers;
            }
        }

        REGISTER_YSON_STRUCT_LITE(TCypressWatchlist);

        static void Register(TRegistrar registrar)
        {
            registrar.Parameter("queues", &TThis::Queues)
                .Default();
            registrar.Parameter("consumers", &TThis::Consumers)
                .Default();
        }
    };

    void ListObjectChangesWatching()
    {
        // First, we collect all queue agent objects from the corresponding master watchlist in each watched cluster.

        std::vector<TFuture<TYPathProxy::TRspGetPtr>> asyncResults;
        for (const auto& cluster : Clusters_) {
            auto proxy = CreateObjectServiceReadProxy(
                GetNativeClientOrThrow(cluster),
                EMasterChannelKind::Follower);
            asyncResults.push_back(proxy.Execute(
                TYPathProxy::Get("//sys/@queue_agent_object_revisions")));
        }

        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        std::vector<TError> clusterRevisionFetchingAlerts;
        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& rspOrError = combinedResults[index];
            const auto& cluster = Clusters_[index];

            if (!rspOrError.IsOK()) {
                clusterRevisionFetchingAlerts.push_back(rspOrError << TErrorAttribute("cluster", cluster));
                continue;
            }

            auto cypressWatchlist = ConvertTo<TCypressWatchlist>(TYsonString(rspOrError.Value()->value()));

            InferChangesFromClusterWatchlist(cluster, std::move(cypressWatchlist));
        }

        if (!clusterRevisionFetchingAlerts.empty()) {
            Alerts_.push_back(
                TError(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchObjectRevisions,
                    "Error retrieving queue agent object revisions from clusters")
                    << clusterRevisionFetchingAlerts);
        }
    }

    void InferChangesFromClusterWatchlist(const TString& cluster, TCypressWatchlist cypressWatchlist)
    {
        // First, we collect all dynamic state objects for which the current Cypress revision
        // is larger than the stored revision.

        if (auto clusterToObjectsIt = ClusterToDynamicStateObjects_.find(cluster); clusterToObjectsIt != ClusterToDynamicStateObjects_.end()) {
            for (const auto& object : clusterToObjectsIt->second) {
                auto& relevantCypressWatchlist = cypressWatchlist.ObjectsByType(object.Type);
                auto cypressObjectIt = relevantCypressWatchlist.find(object.Object.Path);
                if (cypressObjectIt != relevantCypressWatchlist.end()) {
                    if (!object.Revision || cypressObjectIt->second > *object.Revision) {
                        YT_LOG_DEBUG(
                            "Object Cypress revision changed (Object: %v, Revision: %x -> %x)",
                            object.Object,
                            object.Revision,
                            cypressObjectIt->second);
                        ClusterToModifiedObjects_[cluster].push_back(object);
                    }
                    relevantCypressWatchlist.erase(cypressObjectIt);
                } else {
                    YT_LOG_DEBUG(
                        "Object was not found in corresponding watchlist (Object: %v)",
                        object.Object);
                    ObjectsToDelete_.push_back(object);
                }
            }
        }

        // The remaining objects are not present in the current dynamic state, thus they are all new and modified.

        for (const auto& type : TEnumTraits<ECypressSyncObjectType>::GetDomainValues()) {
            for (const auto& [object, revision] : cypressWatchlist.ObjectsByType(type)) {
                TCrossClusterReference objectRef{cluster, object};
                YT_LOG_DEBUG(
                    "Discovered object (Object: %v, Revision: %x)",
                    objectRef,
                    revision);
                ClusterToModifiedObjects_[cluster].push_back({
                    .Object = objectRef,
                    .Type = type,
                    .Revision = revision,
                });
            }
        }
    }

    //! List all objects that appear in the dynamic state.
    void FetchObjectMaps()
    {
        auto asyncQueues = DynamicState_->Queues->Select();
        auto asyncConsumers = DynamicState_->Consumers->Select();
        WaitFor(AllSucceeded(std::vector{asyncQueues.AsVoid(), asyncConsumers.AsVoid()}))
            .ThrowOnError();

        for (const auto& queue : asyncQueues.Get().Value()) {
            ClusterToDynamicStateObjects_[queue.Ref.Cluster].push_back({
                queue.Ref,
                ECypressSyncObjectType::Queue,
                queue.Revision,
                queue.RowRevision});
        }
        for (const auto& consumer : asyncConsumers.Get().Value()) {
            ClusterToDynamicStateObjects_[consumer.Ref.Cluster].push_back({
                consumer.Ref,
                ECypressSyncObjectType::Consumer,
                consumer.Revision,
                consumer.RowRevision});
        }
    }

    static std::vector<TString> GetCypressAttributeNames(const TObject& object)
    {
        switch (object.Type) {
            case ECypressSyncObjectType::Consumer:
                return TConsumerTableRow::GetCypressAttributeNames();
            case ECypressSyncObjectType::Queue:
                return TQueueTableRow::GetCypressAttributeNames();
        }
    }

    //! Fetch attributes for the specified objects and update the corresponding dynamic state rows.
    void FetchAttributes()
    {
        // Fetch attributes for modified objects via batch requests to each cluster.

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        std::vector<TString> clusters;
        for (const auto& [cluster, modifiedObjects] : ClusterToModifiedObjects_) {
            auto proxy = CreateObjectServiceReadProxy(
                GetNativeClientOrThrow(cluster),
                EMasterChannelKind::Follower);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& object : modifiedObjects) {
                auto req = TYPathProxy::Get(object.Object.Path + "/@");
                ToProto(
                    req->mutable_attributes()->mutable_keys(),
                    GetCypressAttributeNames(object));
                batchReq->AddRequest(req);
            }
            asyncResults.push_back(batchReq->Invoke());
            clusters.push_back(cluster);
        }
        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        // Create rows for the modified objects with new attribute values and an increased row revision.

        std::vector<TError> clusterAttributeFetchingAlerts;
        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& batchRsp = combinedResults[index];
            const auto& cluster = clusters[index];
            if (!batchRsp.IsOK()) {
                clusterAttributeFetchingAlerts.push_back(GetCumulativeError(batchRsp) << TErrorAttribute("cluster", cluster));
                continue;
            }
            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToModifiedObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    YT_LOG_ERROR(
                        responseOrError,
                        "Error fetching attributes for object ",
                        object.Object);
                    RowsWithErrors_.AppendObjectWithError(object, responseOrError);
                    continue;
                }
                auto attributes = ConvertToAttributes(TYsonString(responseOrError.Value()->value()));
                YT_LOG_DEBUG(
                    "Fetched updated attributes (Object: %v, Attributes: %v)",
                    object.Object,
                    ConvertToYsonString(attributes, EYsonFormat::Text));

                try {
                    RowsToWrite_.AppendObject(object, attributes);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error converting attributes to object %v",
                        object.Object);
                    RowsWithErrors_.AppendObjectWithError(object, ex);
                }
            }
        }

        if (!clusterAttributeFetchingAlerts.empty()) {
            Alerts_.push_back(
                TError(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchAttributes,
                    "Error fetching attributes from clusters")
                    << clusterAttributeFetchingAlerts);
        }
    }

    //! Write rows to dynamic state.
    void WriteRows()
    {
        if (!RowsWithErrors_.Empty()) {
            YT_LOG_DEBUG(
                "Some rows contain synchronization errors (QueueCount: %v, ConsumerCount: %v)",
                RowsWithErrors_.QueueRows.size(),
                RowsWithErrors_.ConsumerRows.size());
            RowsToWrite_.MergeWith(RowsWithErrors_);
        }
        YT_LOG_DEBUG(
            "Writing updated rows (QueueCount: %v, ConsumerCount: %v)",
            RowsToWrite_.QueueRows.size(),
            RowsToWrite_.ConsumerRows.size());
        WaitFor(AllSucceeded(std::vector{
            DynamicState_->Consumers->Insert(RowsToWrite_.ConsumerRows),
            DynamicState_->Queues->Insert(RowsToWrite_.QueueRows)}))
            .ThrowOnError();
    }

    //! Delete objects from dynamic state.
    void DeleteObjects()
    {
        for (const auto& object : ObjectsToDelete_) {
            RowsToDelete_.AppendObjectKey(object);
        }
        DeleteRows();
    }

    //! Delete key rows from dynamic state.
    void DeleteRows()
    {
        if (RowsToDelete_.Empty()) {
            return;
        }

        YT_LOG_DEBUG(
            "Deleting rows (QueueCount: %v, ConsumerCount: %v)",
            RowsToDelete_.QueueRows.size(),
            RowsToDelete_.ConsumerRows.size());
        WaitFor(AllSucceeded(std::vector{
            DynamicState_->Queues->Delete(RowsToDelete_.QueueRows),
            DynamicState_->Consumers->Delete(RowsToDelete_.ConsumerRows)}))
            .ThrowOnError();
    }

    NNative::IClientPtr GetNativeClientOrThrow(const TString& cluster) const
    {
        try {
            return AssertNativeClient(ClientDirectory_->GetClientOrThrow(cluster));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating client for cluster %Qv", cluster) << ex;
        }
    }
};

class TCypressSynchronizer
    : public ICypressSynchronizer
{
public:
    TCypressSynchronizer(
        TCypressSynchronizerConfigPtr config,
        IInvokerPtr controlInvoker,
        TDynamicStatePtr dynamicState,
        TClientDirectoryPtr clientDirectory)
        : Config_(std::move(config))
        , DynamicConfig_(New<TCypressSynchronizerDynamicConfig>())
        , ControlInvoker_(std::move(controlInvoker))
        , DynamicState_(std::move(dynamicState))
        , ClientDirectory_(std::move(clientDirectory))
        , PassExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TCypressSynchronizer::Pass, MakeWeak(this)),
            DynamicConfig_->PassPeriod))
        , OrchidService_(IYPathService::FromProducer(BIND(&TCypressSynchronizer::BuildOrchid, MakeWeak(this)))->Via(ControlInvoker_))
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    void Start() override
    {
        Active_ = true;

        PassExecutor_->Start();
    }

    void Stop() override
    {
        // NB: We can't have context switches happen in this callback, so sync operations could potentially be performed
        // after a call to CypressSynchronizer::Stop().
        YT_UNUSED_FUTURE(PassExecutor_->Stop());

        Active_ = false;
    }

    //! Perform a pass iteration which finds out which objects have changed since the last round
    //! and updates the corresponding rows in the dynamic state.
    void Pass()
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("CypressSynchronizer"));

        if (!DynamicConfig_->Enable) {
            YT_LOG_DEBUG("Pass skipped");
            Alerts_.clear();
            return;
        }

        PassInstant_ = TInstant::Now();
        ++PassIndex_;

        YT_LOG_DEBUG("Pass started (PassIndex: %v)", PassIndex_);
        try {
            auto alerts = TCypressSynchronizerPassSession(
                DynamicConfig_->Policy,
                DynamicConfig_->Clusters,
                DynamicState_,
                ClientDirectory_,
                Logger.WithTag("PassIndex: %v", PassIndex_))
                .Build();
            PassError_ = TError();
            Alerts_.swap(alerts);
        } catch (const std::exception& ex) {
            PassError_ = TError(ex);
            auto alert = TError(
                NAlerts::EErrorCode::CypressSynchronizerPassFailed,
                "Error performing cypress synchronizer pass")
                << TError(ex);
            Alerts_ = {alert};
        }

        YT_LOG_DEBUG("Pass finished (PassIndex: %v)", PassIndex_);
    }

    void OnDynamicConfigChanged(
        const TCypressSynchronizerDynamicConfigPtr& oldConfig,
        const TCypressSynchronizerDynamicConfigPtr& newConfig) override
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        DynamicConfig_ = newConfig;

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        YT_LOG_DEBUG(
            "Updated Cypress synchronizer dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    void PopulateAlerts(std::vector<TError>* alerts) const override
    {
        WaitFor(
            BIND(&TCypressSynchronizer::DoPopulateAlerts, MakeStrong(this), alerts)
                .AsyncVia(ControlInvoker_)
                .Run())
            .ThrowOnError();
    }

private:
    const TCypressSynchronizerConfigPtr Config_;
    TCypressSynchronizerDynamicConfigPtr DynamicConfig_;
    const IInvokerPtr ControlInvoker_;
    const TDynamicStatePtr DynamicState_;
    const TClientDirectoryPtr ClientDirectory_;
    const TPeriodicExecutorPtr PassExecutor_;
    const IYPathServicePtr OrchidService_;

    //! Whether this instance is actively performing passes.
    std::atomic<bool> Active_ = false;
    //! Current pass iteration error.
    TError PassError_;
    //! Current pass iteration instant.
    TInstant PassInstant_ = TInstant::Zero();
    //! Index of the current pass iteration.
    i64 PassIndex_ = -1;

    std::vector<TError> Alerts_;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        BuildYsonFluently(consumer).BeginMap()
            .Item("active").Value(Active_)
            .Item("pass_instant").Value(PassInstant_)
            .Item("pass_index").Value(PassIndex_)
            .Item("pass_error").Value(PassError_)
        .EndMap();
    }

    void DoPopulateAlerts(std::vector<TError>* alerts) const
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        alerts->insert(alerts->end(), Alerts_.begin(), Alerts_.end());
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressSynchronizer)

ICypressSynchronizerPtr CreateCypressSynchronizer(
    TCypressSynchronizerConfigPtr config,
    IInvokerPtr controlInvoker,
    TDynamicStatePtr dynamicState,
    TClientDirectoryPtr clientDirectory)
{
    return New<TCypressSynchronizer>(
        std::move(config),
        std::move(controlInvoker),
        std::move(dynamicState),
        std::move(clientDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
