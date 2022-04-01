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

namespace {

////////////////////////////////////////////////////////////////////////////////

TRowRevision NextRowRevision(const std::optional<TRowRevision> rowRevision)
{
    return rowRevision.value_or(0) + 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

DEFINE_ENUM(ECypressSyncObjectType,
    (Queue)
    (Consumer)
);

struct TObject
{
    //! The location (cluster, path) of the object in question.
    TCrossClusterReference Object;
    ECypressSyncObjectType Type;
    //! The revision of the corresponding cypress node.
    std::optional<NHydra::TRevision> Revision;
    //! The internal revision of the corresponding dynamic state row.
    std::optional<TRowRevision> RowRevision;
};

////////////////////////////////////////////////////////////////////////////////

class TPollingCypressSynchronizer
    : public ICypressSynchronizer
{
public:
    TPollingCypressSynchronizer(
        TCypressSynchronizerConfigPtr config,
        IInvokerPtr controlInvoker,
        TDynamicStatePtr dynamicState,
        TClientDirectoryPtr clientDirectory)
        : Config_(std::move(config))
        , DynamicConfig_(New<TCypressSynchronizerDynamicConfig>())
        , ControlInvoker_(std::move(controlInvoker))
        , DynamicState_(std::move(dynamicState))
        , ClientDirectory_(std::move(clientDirectory))
        , SyncExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TPollingCypressSynchronizer::Poll, MakeWeak(this)),
            DynamicConfig_->PollPeriod))
        , OrchidService_(IYPathService::FromProducer(BIND(&TPollingCypressSynchronizer::BuildOrchid, MakeWeak(this)))->Via(ControlInvoker_))
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    void Start() override
    {
        Active_ = true;

        SyncExecutor_->Start();
    }

    void Stop() override
    {
        // NB: We can't have context switches happen in this callback, so sync operations could potentially be performed
        // after a call to CypressSynchronizer::Stop().
        SyncExecutor_->Stop();

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
            auto objectMaps = FetchObjectMaps();
            auto modifiedObjects = ListModifiedObjectsByCluster(objectMaps);
            auto updatedAttributes = FetchAttributes(modifiedObjects);
            WriteRows(updatedAttributes);
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

        SyncExecutor_->SetPeriod(newConfig->PollPeriod);

        YT_LOG_DEBUG(
            "Updated cypress synchronizer dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

private:
    const TCypressSynchronizerConfigPtr Config_;
    TCypressSynchronizerDynamicConfigPtr DynamicConfig_;
    const IInvokerPtr ControlInvoker_;
    const TDynamicStatePtr DynamicState_;
    const TClientDirectoryPtr ClientDirectory_;
    const TPeriodicExecutorPtr SyncExecutor_;
    const IYPathServicePtr OrchidService_;

    //! Whether this instance is actively performing polling.
    std::atomic<bool> Active_ = false;
    //! Current poll iteration error.
    TError PollError_;
    //! Current poll iteration instant.
    TInstant PollInstant_ = TInstant::Zero();
    //! Index of the current poll iteration.
    i64 PollIndex_ = 0;

    using TObjectMap = THashMap<TString, std::vector<TObject>>;

    //! Fetch revisions for all objects in the dynamic state and return the ones with a new cypress revision.
    TObjectMap ListModifiedObjectsByCluster(const TObjectMap& clusterToObjects) const
    {
        // Fetch cypress revisions for all objects in dynamic state.

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        std::vector<TString> clusters;
        for (const auto& [cluster, objects] : clusterToObjects) {
            IChannelPtr channel;
            try {
                auto client = AssertNativeClient(ClientDirectory_->GetClientOrThrow(cluster));
                channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error connecting to cluster %v", cluster) << ex;
            }
            TObjectServiceProxy proxy(channel);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& object : objects) {
                batchReq->AddRequest(TYPathProxy::Get(object.Object.Path + "/@revision"));
            }
            asyncResults.push_back(batchReq->Invoke());
            clusters.push_back(cluster);
        }

        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        // Collect all objects for which the current cypress revision is larger than the stored revision.

        TObjectMap clusterToModifiedObjects;
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
                auto object = clusterToObjects.at(cluster)[objectIndex];
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
                        "Object cypress revision changed (PollIndex: %v, Object: %v, Revision: %v -> %v)",
                        PollIndex_,
                        object.Object,
                        object.Revision,
                        revision);
                    object.Revision = revision;
                    clusterToModifiedObjects[cluster].push_back(std::move(object));
                }
            }
        }

        return clusterToModifiedObjects;
    }

    //! List all objects that appear in the dynamic state.
    TObjectMap FetchObjectMaps() const
    {
        auto asyncQueues = DynamicState_->Queues->Select();
        auto asyncConsumers = DynamicState_->Consumers->Select();
        WaitFor(AllSucceeded(std::vector{asyncQueues.AsVoid(), asyncConsumers.AsVoid()}))
            .ThrowOnError();

        TObjectMap clusterToObjects;
        for (const auto& queue: asyncQueues.Get().Value()) {
            clusterToObjects[queue.Queue.Cluster].push_back({
                queue.Queue,
                ECypressSyncObjectType::Queue,
                queue.Revision,
                queue.RowRevision});
        }
        for (const auto& consumer : asyncConsumers.Get().Value()) {
            clusterToObjects[consumer.Consumer.Cluster].push_back({
                consumer.Consumer,
                ECypressSyncObjectType::Consumer,
                consumer.Revision,
                consumer.RowRevision});
        }
        return clusterToObjects;
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

    struct TObjectRowList
    {
        std::vector<TQueueTableRow> queueRows;
        std::vector<TConsumerTableRow> consumerRows;
    };

    static void AppendObjectToObjectRowList(
        const TObject& object,
        const IAttributeDictionaryPtr& attributes,
        TObjectRowList& objectRowList)
    {
        switch (object.Type) {
            case ECypressSyncObjectType::Queue:
                objectRowList.queueRows.push_back(TQueueTableRow::FromAttributeDictionary(
                    object.Object,
                    NextRowRevision(object.RowRevision),
                    attributes));
                break;
            case ECypressSyncObjectType::Consumer:
                objectRowList.consumerRows.push_back(TConsumerTableRow::FromAttributeDictionary(
                    object.Object,
                    NextRowRevision(object.RowRevision),
                    attributes));
                break;
        }
    }

    //! Fetch attributes for the specified objects and update the corresponding dynamic state rows.
    virtual TObjectRowList FetchAttributes(const TObjectMap& clusterToModifiedObjects) const
    {
        // Fetch attributes for modified objects via batch requests to each cluster.

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        std::vector<TString> clusters;
        for (const auto& [cluster, modifiedObjects] : clusterToModifiedObjects) {
            IChannelPtr channel;
            try {
                auto client = AssertNativeClient(ClientDirectory_->GetClientOrThrow(cluster));
                channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error connecting to cluster %v", cluster) << ex;
            }
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

        TObjectRowList objectRowList;
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
                const auto& object = clusterToModifiedObjects.at(cluster)[objectIndex];
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

                AppendObjectToObjectRowList(object, attributes, objectRowList);
            }
        }

        return objectRowList;
    }

    //! Write rows to dynamic state.
    void WriteRows(const TObjectRowList& objectRowList)
    {
        YT_LOG_DEBUG(
            "Writing updated rows (PollIndex: %v, QueueCount: %v, ConsumerCount: %v)",
            PollIndex_,
            objectRowList.queueRows.size(),
            objectRowList.consumerRows.size());
        WaitFor(AllSucceeded(std::vector{
            DynamicState_->Consumers->Insert(objectRowList.consumerRows),
            DynamicState_->Queues->Insert(objectRowList.queueRows)}))
            .ThrowOnError();
    }

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

DEFINE_REFCOUNTED_TYPE(TPollingCypressSynchronizer)

ICypressSynchronizerPtr CreatePollingCypressSynchronizer(
    TCypressSynchronizerConfigPtr config,
    IInvokerPtr controlInvoker,
    TDynamicStatePtr dynamicState,
    TClientDirectoryPtr clientDirectory)
{
    return New<TPollingCypressSynchronizer>(
        std::move(config),
        std::move(controlInvoker),
        std::move(dynamicState),
        std::move(clientDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
