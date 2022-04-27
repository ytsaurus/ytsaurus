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

class TCypressSynchronizerPollSession final
{
public:
    TCypressSynchronizerPollSession(
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

    void Build()
    {
        FetchObjectMaps();
        ListObjectChanges();
        DeleteObjects();
        FetchAttributes();
        WriteRows();
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
        std::vector<TQueueTableRow> queueRows;
        std::vector<TConsumerTableRow> consumerRows;

        void AppendObject(const TObject& object, const IAttributeDictionaryPtr& attributes)
        {
            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    queueRows.push_back(TQueueTableRow::FromAttributeDictionary(
                        object.Object,
                        NextRowRevision(object.RowRevision),
                        attributes));
                    break;
                case ECypressSyncObjectType::Consumer:
                    consumerRows.push_back(TConsumerTableRow::FromAttributeDictionary(
                        object.Object,
                        NextRowRevision(object.RowRevision),
                        attributes));
                    break;
            }
        }

        void AppendObjectKey(const TObject& object)
        {
            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    queueRows.push_back({.Queue = object.Object});
                    break;
                case ECypressSyncObjectType::Consumer:
                    consumerRows.push_back({.Consumer = object.Object});
                    break;
            }
        }
    };

    // Session state.
    
    TObjectMap ClusterToDynamicStateObjects_;
    TObjectMap ClusterToModifiedObjects_;
    std::vector<TObject> ObjectsToDelete_;
    TObjectRowList RowsToDelete_;
    TObjectRowList RowsToWrite_;

    void ListObjectChanges()
    {
        switch (Policy_) {
            case ECypressSynchronizerPolicy::Polling:
                ListObjectChangesPolling();
            case ECypressSynchronizerPolicy::Watching:
                ListObjectChangesWatching();
        }
    }

    //! Fetch revisions for all objects in the dynamic state and return the ones with a new Cypress revision.
    void ListObjectChangesPolling()
    {
        // Fetch Cypress revisions for all objects in dynamic state.

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        std::vector<TString> clusters;
        for (const auto& [cluster, objects] : ClusterToDynamicStateObjects_) {
            const auto& channel = GetMasterChannelOrThrow(cluster);
            TObjectServiceProxy proxy(channel);
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

        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& batchRsp = combinedResults[index];
            const auto& cluster = clusters[index];
            if (!batchRsp.IsOK()) {
                // TODO(achulkov2): Propagate this error to objects for later introspection.
                YT_LOG_ERROR(
                    GetCumulativeError(batchRsp),
                    "Error fetching object revisions from cluster %v",
                    cluster);
                continue;
            }
            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToDynamicStateObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    // TODO(achulkov2): Propagate this error to the object for later introspection.
                    YT_LOG_DEBUG(
                        responseOrError,
                        "Error fetching revision for object %v",
                        object.Object);
                    continue;
                }

                std::optional<NHydra::TRevision> revision;
                try {
                    revision = ConvertTo<NHydra::TRevision>(TYsonString(responseOrError.Value()->value()));
                } catch (const std::exception& ex) {
                    // TODO(achulkov2): Propagate this error to the object for later introspection.
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing revision for object %v",
                        object.Object);
                    continue;
                }
                if (!object.Revision || *object.Revision < *revision) {
                    YT_LOG_DEBUG(
                        "Object Cypress revision changed (Object: %v, Revision: %llx -> %llx)",
                        object.Object,
                        object.Revision,
                        revision);
                    ClusterToModifiedObjects_[cluster].push_back(object);
                }
            }
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
            const auto& channel = GetMasterChannelOrThrow(cluster);
            TObjectServiceProxy proxy(channel);
            asyncResults.push_back(proxy.Execute(
                TYPathProxy::Get("//sys/@queue_agent_object_revisions")));
        }

        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& rspOrError = combinedResults[index];
            const auto& cluster = Clusters_[index];

            if (!rspOrError.IsOK()) {
                YT_LOG_ERROR(
                    rspOrError,
                    "Error retrieving queue agent object revisions from cluster (Cluster: %v)",
                    cluster);
                continue;
            }

            auto cypressWatchlist = ConvertTo<TCypressWatchlist>(TYsonString(rspOrError.Value()->value()));

            InferChangesFromClusterWatchlist(cluster, std::move(cypressWatchlist));
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
                            "Object Cypress revision changed (Object: %v, Revision: %llx -> %llx)",
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
                    "Discovered object (Object: %v, Revision: %llx)",
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
            ClusterToDynamicStateObjects_[queue.Queue.Cluster].push_back({
                queue.Queue,
                ECypressSyncObjectType::Queue,
                queue.Revision,
                queue.RowRevision});
        }
        for (const auto& consumer : asyncConsumers.Get().Value()) {
            ClusterToDynamicStateObjects_[consumer.Consumer.Cluster].push_back({
                consumer.Consumer,
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
            const auto& channel = GetMasterChannelOrThrow(cluster);
            TObjectServiceProxy proxy(channel);
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

        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& batchRsp = combinedResults[index];
            const auto& cluster = clusters[index];
            if (!batchRsp.IsOK()) {
                // TODO(achulkov2): Propagate this error to objects for later introspection.
                YT_LOG_ERROR(
                    GetCumulativeError(batchRsp),
                    "Error fetching object attributes from cluster %v",
                    cluster);
                continue;
            }
            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToModifiedObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    // TODO(achulkov2): Propagate this error to the object for later introspection.
                    YT_LOG_ERROR(
                        responseOrError,
                        "Error fetching attributes for object ",
                        object.Object);
                    continue;
                }
                auto attributes = ConvertToAttributes(TYsonString(responseOrError.Value()->value()));
                YT_LOG_DEBUG(
                    "Fetched updated attributes (Object: %v, Attributes: %v)",
                    object.Object,
                    ConvertToYsonString(attributes, EYsonFormat::Text));

                RowsToWrite_.AppendObject(object, attributes);
            }
        }
    }

    //! Write rows to dynamic state.
    void WriteRows()
    {
        YT_LOG_DEBUG(
            "Writing updated rows (QueueCount: %v, ConsumerCount: %v)",
            RowsToWrite_.queueRows.size(),
            RowsToWrite_.consumerRows.size());
        WaitFor(AllSucceeded(std::vector{
            DynamicState_->Consumers->Insert(RowsToWrite_.consumerRows),
            DynamicState_->Queues->Insert(RowsToWrite_.queueRows)}))
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
        if (RowsToDelete_.consumerRows.empty() && RowsToDelete_.queueRows.empty()) {
            return;
        }

        YT_LOG_DEBUG(
            "Deleting rows (QueueCount: %v, ConsumerCount: %v)",
            RowsToDelete_.queueRows.size(),
            RowsToDelete_.consumerRows.size());
        WaitFor(AllSucceeded(std::vector{
            DynamicState_->Consumers->Delete(RowsToDelete_.consumerRows),
            DynamicState_->Queues->Delete(RowsToDelete_.queueRows)}))
            .ThrowOnError();
    }

    IChannelPtr GetMasterChannelOrThrow(const TString& cluster) const
    {
        try {
            const auto& client = AssertNativeClient(ClientDirectory_->GetClientOrThrow(cluster));
            return client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating channel for cluster %v", cluster) << ex;
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
        , PollExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TCypressSynchronizer::Poll, MakeWeak(this)),
            DynamicConfig_->PollPeriod))
        , OrchidService_(IYPathService::FromProducer(BIND(&TCypressSynchronizer::BuildOrchid, MakeWeak(this)))->Via(ControlInvoker_))
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    void Start() override
    {
        Active_ = true;

        PollExecutor_->Start();
    }

    void Stop() override
    {
        // NB: We can't have context switches happen in this callback, so sync operations could potentially be performed
        // after a call to CypressSynchronizer::Stop().
        PollExecutor_->Stop();

        Active_ = false;
    }

    //! Perform a polling round which finds out which objects have changed since the last round
    //! and updates the corresponding rows in the dynamic state.
    void Poll()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("CypressSynchronizer"));

        if (!DynamicConfig_->Enable) {
            YT_LOG_DEBUG("Polling iteration skipped");
            return;
        }

        PollInstant_ = TInstant::Now();
        ++PollIndex_;

        YT_LOG_DEBUG("Polling round started (PollIndex: %v)", PollIndex_);
        try {
            TCypressSynchronizerPollSession(
                DynamicConfig_->Policy,
                DynamicConfig_->Clusters,
                DynamicState_,
                ClientDirectory_,
                Logger.WithTag("PollIndex: %v", PollIndex_))
                .Build();
            PollError_ = TError();
        } catch (const std::exception& ex) {
            PollError_ = TError(ex);
            YT_LOG_ERROR(ex, "Error performing polling round");
        }

        YT_LOG_DEBUG("Polling round finished (PollIndex: %v)", PollIndex_);
    }

    void OnDynamicConfigChanged(
        const TCypressSynchronizerDynamicConfigPtr& oldConfig,
        const TCypressSynchronizerDynamicConfigPtr& newConfig) override
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        DynamicConfig_ = newConfig;

        PollExecutor_->SetPeriod(newConfig->PollPeriod);

        YT_LOG_DEBUG(
            "Updated Cypress synchronizer dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

private:
    const TCypressSynchronizerConfigPtr Config_;
    TCypressSynchronizerDynamicConfigPtr DynamicConfig_;
    const IInvokerPtr ControlInvoker_;
    const TDynamicStatePtr DynamicState_;
    const TClientDirectoryPtr ClientDirectory_;
    const TPeriodicExecutorPtr PollExecutor_;
    const IYPathServicePtr OrchidService_;

    //! Whether this instance is actively performing polling.
    std::atomic<bool> Active_ = false;
    //! Current poll iteration error.
    TError PollError_;
    //! Current poll iteration instant.
    TInstant PollInstant_ = TInstant::Zero();
    //! Index of the current poll iteration.
    i64 PollIndex_ = -1;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        BuildYsonFluently(consumer).BeginMap()
            .Item("active").Value(Active_)
            .Item("poll_instant").Value(PollInstant_)
            .Item("poll_index").Value(PollIndex_)
            .Item("poll_error").Value(PollError_)
        .EndMap();
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
