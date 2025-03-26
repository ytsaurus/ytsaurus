#include "cypress_synchronizer.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NQueueAgent {

using namespace NAlertManager;
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

static constexpr auto& Logger = CypressSynchronizerLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECypressSyncObjectType,
    (Queue)
    (Consumer)
);

namespace {

////////////////////////////////////////////////////////////////////////////////

TRowRevision NextRowRevision(const std::optional<TRowRevision> rowRevision)
{
    return TRowRevision(rowRevision.value_or(NullRowRevision).Underlying() + 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

struct TObject
{
    //! The location (cluster, path) of the object in question.
    TCrossClusterReference Object;
    ECypressSyncObjectType Type;
    //! Master object type.
    std::optional<EObjectType> ObjectType;
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
        TCypressSynchronizerDynamicConfigPtr dynamicConfigSnapshot,
        TDynamicStatePtr dynamicState,
        TClientDirectoryPtr clientDirectory,
        IAlertCollectorPtr alertCollector,
        const NLogging::TLogger& logger)
        : DynamicConfigSnapshot_(std::move(dynamicConfigSnapshot))
        , DynamicState_(std::move(dynamicState))
        , ClientDirectory_(std::move(clientDirectory))
        , AlertCollector_(std::move(alertCollector))
        , Logger(logger)
    { }

    void Build()
    {
        if (DynamicConfigSnapshot_->Policy == ECypressSynchronizerPolicy::Polling && (DynamicConfigSnapshot_->PollReplicatedObjects || DynamicConfigSnapshot_->WriteReplicatedTableMapping)) {
            THROW_ERROR_EXCEPTION("Cypress synchronizer cannot work with replicated objects in polling mode");
        }

        if (DynamicConfigSnapshot_->WriteReplicatedTableMapping && !DynamicConfigSnapshot_->PollReplicatedObjects) {
            THROW_ERROR_EXCEPTION("Cypress synchronizer cannot build replicated table mapping with replicated object polling disabled");
        }

        FetchObjectMaps();
        if (DynamicConfigSnapshot_->PollReplicatedObjects) {
            YT_LOG_DEBUG("Polling replicated objects is enabled");
            FetchObjectsFromRegistrationTable();
        } else {
            YT_LOG_DEBUG("Polling replicated objects is disabled");
        }

        ListObjectChanges();

        DeleteRows();
        FetchAttributes();
        WriteRows();
    }

private:
    const TCypressSynchronizerDynamicConfigPtr DynamicConfigSnapshot_;
    const TDynamicStatePtr DynamicState_;
    const TClientDirectoryPtr ClientDirectory_;
    const IAlertCollectorPtr AlertCollector_;
    const NLogging::TLogger Logger;

    using TObjectMap = THashMap<TString, std::vector<TObject>>;

    struct TObjectHashMap
        : public THashMap<TString, THashMap<TString, TObject>>
    {
        bool Contains(const TCrossClusterReference& objectRef) const
        {
            auto clusterToObjectsIt = find(objectRef.Cluster);
            if (clusterToObjectsIt == end()) {
                return false;
            }

            auto objectIt = clusterToObjectsIt->second.find(objectRef.Path);
            return objectIt != clusterToObjectsIt->second.end();
        }

        void Insert(const TObject& object)
        {
            (*this)[object.Object.Cluster][object.Object.Path] = object;
        }
    };

    struct TObjectRowList
    {
        //! Queue agent dynamic state modifications.
        //! These tables are only used by this queue agent instance.
        std::vector<TQueueTableRow> QueueRows;
        std::vector<TConsumerTableRow> ConsumerRows;

        //! Modifications for the general cross-cluster [chaos] replicated table -> replicas mapping table.
        //! This table is used by proxies of all clusters.
        std::vector<TReplicatedTableMappingTableRow> ReplicatedTableMappingRows;

        // NB: Must provide a strong exception-safety guarantee.
        void AppendObject(const TObject& object, const IAttributeDictionaryPtr& attributes, const std::string& chaosReplicatedTableQueueAgentStage)
        {
            auto fillChaosReplicatedTableQueueAgentStage = [=] (auto& row) {
                if (row.ObjectType && *row.ObjectType == EObjectType::ChaosReplicatedTable && !row.QueueAgentStage) {
                    row.QueueAgentStage = chaosReplicatedTableQueueAgentStage;
                }
            };

            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    QueueRows.push_back(TQueueTableRow::FromAttributeDictionary(
                        object.Object,
                        NextRowRevision(object.RowRevision),
                        attributes));
                    fillChaosReplicatedTableQueueAgentStage(QueueRows.back());
                    break;
                case ECypressSyncObjectType::Consumer:
                    ConsumerRows.push_back(TConsumerTableRow::FromAttributeDictionary(
                        object.Object,
                        NextRowRevision(object.RowRevision),
                        attributes));
                    fillChaosReplicatedTableQueueAgentStage(ConsumerRows.back());
                    break;
            }
        }

        // NB: Must provide a strong exception-safety guarantee.
        void AppendPotentiallyReplicatedObject(const TObject& object, const IAttributeDictionaryPtr& attributes) {
            auto tableRow =
                TReplicatedTableMappingTableRow::FromAttributeDictionary(object.Object, attributes);
            const auto& type = tableRow.ObjectType;
            if (IsReplicatedTableObjectType(type)) {
                ReplicatedTableMappingRows.push_back(std::move(tableRow));
            }
        }

        void AppendObjectWithError(
            const TObject& object,
            const TError& error,
            std::optional<NHydra::TRevision> revision = std::nullopt)
        {
            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    QueueRows.push_back({
                        .Ref = object.Object,
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .Revision = revision,
                        .SynchronizationError = error,
                    });
                    break;
                case ECypressSyncObjectType::Consumer:
                    ConsumerRows.push_back({
                        .Ref = object.Object,
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .Revision = revision,
                        .SynchronizationError = error,
                    });
                    break;
            }
        }

        void AppendObjectWithErrorAndBasicAttributes(
            const TObject& object,
            const IAttributeDictionaryPtr& attributes,
            const std::string& chaosReplicatedTableQueueAgentStage,
            const TError& error,
            const NLogging::TLogger& logger)
        {
            const auto& Logger = logger;

            auto fillFromAttribute = [&] <typename TTo> (TStringBuf attribute, std::optional<TTo>& attributeField) {
                try {
                    attributeField = attributes->Find<TTo>(attribute);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(ex, "Error parsing attribute %Qv",
                        attribute);
                    attributeField = std::nullopt;
                }
            };

            auto fillBasicFieldsFromAttributes = [&] (auto& row) {
                fillFromAttribute("attribute_revision", row.Revision);
                fillFromAttribute("type", row.ObjectType);
                fillFromAttribute("queue_agent_stage", row.QueueAgentStage);
                if (row.QueueAgentStage) {
                    return;
                }
                if (row.ObjectType && *row.ObjectType == EObjectType::ChaosReplicatedTable && !chaosReplicatedTableQueueAgentStage.empty()) {
                    row.QueueAgentStage = chaosReplicatedTableQueueAgentStage;
                }
            };

            switch (object.Type) {
                case ECypressSyncObjectType::Queue:
                    QueueRows.push_back({
                        .Ref = object.Object,
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .SynchronizationError = error,
                    });
                    fillBasicFieldsFromAttributes(QueueRows.back());
                    break;
                case ECypressSyncObjectType::Consumer:
                    ConsumerRows.push_back({
                        .Ref = object.Object,
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .SynchronizationError = error,
                    });
                    fillBasicFieldsFromAttributes(ConsumerRows.back());
                    break;
            }
        }

        void AppendReplicatedObjectWithError(
            const TObject& object,
            const TError& error,
            std::optional<NHydra::TRevision> revision = std::nullopt)
        {
            ReplicatedTableMappingRows.push_back({
                .Ref = object.Object,
                .Revision = revision,
                .SynchronizationError = error,
            });
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

        void AppendReplicatedObjectKey(const TObject& object)
        {
            ReplicatedTableMappingRows.push_back({.Ref = object.Object});
        }

        void MergeWith(const TObjectRowList& rhs)
        {
            QueueRows.insert(QueueRows.end(), rhs.QueueRows.begin(), rhs.QueueRows.end());
            ConsumerRows.insert(ConsumerRows.end(), rhs.ConsumerRows.begin(), rhs.ConsumerRows.end());
            ReplicatedTableMappingRows.insert(ReplicatedTableMappingRows.end(), rhs.ReplicatedTableMappingRows.begin(), rhs.ReplicatedTableMappingRows.end());
        }

        bool Empty() const
        {
            return QueueRows.empty() && ConsumerRows.empty() && ReplicatedTableMappingRows.empty();
        }
    };

    // Session state.

    TObjectMap ClusterToDynamicStateObjects_;
    TObjectHashMap ClusterToPolledObjects_;
    TObjectMap ClusterToModifiedObjects_;
    TObjectRowList RowsWithErrors_;
    TObjectRowList RowsToDelete_;
    TObjectRowList RowsToWrite_;

    void ListObjectChanges()
    {
        switch (DynamicConfigSnapshot_->Policy) {
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
        std::vector<std::string> clusters;
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

        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& batchRsp = combinedResults[index];
            const auto& cluster = clusters[index];
            if (!batchRsp.IsOK()) {
                AlertCollector_->StageAlert(CreateAlert(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchObjectRevisions,
                    "Error fetching object revisions from clusters",
                    /*tags*/ {{"cluster", cluster}},
                    GetCumulativeError(batchRsp)));
                continue;
            }
            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToDynamicStateObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        responseOrError,
                        "Error fetching revision (Object: %v)",
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
                        "Error parsing revision (Object: %v)",
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
        for (const auto& cluster : DynamicConfigSnapshot_->Clusters) {
            auto proxy = CreateObjectServiceReadProxy(
                GetNativeClientOrThrow(cluster),
                EMasterChannelKind::Follower);
            asyncResults.push_back(proxy.Execute(
                TYPathProxy::Get("//sys/@queue_agent_object_revisions")));
        }

        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& rspOrError = combinedResults[index];
            const auto& cluster = DynamicConfigSnapshot_->Clusters[index];

            if (!rspOrError.IsOK()) {
                AlertCollector_->StageAlert(CreateAlert(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchObjectRevisions,
                    "Error retrieving queue agent object revisions from clusters",
                    /*tags*/ {{"cluster", cluster}},
                    rspOrError));
                continue;
            }

            auto cypressWatchlist = ConvertTo<TCypressWatchlist>(TYsonString(rspOrError.Value()->value()));

            InferChangesFromClusterWatchlist(cluster, std::move(cypressWatchlist));
        }
    }

    //! Infers which objects for a given cluster were modified and which should be deleted in this Cypress synchronizer pass.
    //! Modified:
    //!    1) Objects present in watchlist and in current dynamic state, for which we detect a revision increase.
    //!    2) Objects present in watchlist and not in the current dynamic state.
    //!    3) Objects polled from registration table.
    //! Deleted (from dynamic state):
    //!    1) Objects present in the current dynamic state and not present in the watchlist AND in the set of polled objects.
    //! Deleted (from registration table mapping):
    //!    1) Objects present in the current dynamic state and not present in set of polled objects.
    void InferChangesFromClusterWatchlist(const std::string& cluster, TCypressWatchlist cypressWatchlist)
    {
        // First, we collect all dynamic state objects for which the current Cypress revision
        // is larger than the stored revision.

        // Objects for which we can infer from the watchlist whether they were modified or not.
        // Used for adding objects polled from the registration table.
        THashSet<TCrossClusterReference> watchedObjects;

        if (auto clusterToObjectsIt = ClusterToDynamicStateObjects_.find(cluster); clusterToObjectsIt != ClusterToDynamicStateObjects_.end()) {
            for (const auto& object : clusterToObjectsIt->second) {
                auto& relevantCypressWatchlist = cypressWatchlist.ObjectsByType(object.Type);
                auto cypressObjectIt = relevantCypressWatchlist.find(object.Object.Path);
                auto isPolledObject = ClusterToPolledObjects_.Contains(object.Object);
                if (cypressObjectIt != relevantCypressWatchlist.end()) {
                    if (!object.Revision || cypressObjectIt->second > *object.Revision) {
                        YT_LOG_DEBUG(
                            "Object Cypress revision changed (Object: %v, Revision: %x -> %x)",
                            object.Object,
                            object.Revision,
                            cypressObjectIt->second);
                        ClusterToModifiedObjects_[cluster].push_back(object);
                        watchedObjects.insert(object.Object);
                    } else if (object.ObjectType && !IsReplicatedTableObjectType(*object.ObjectType)) {
                        // Besides actually modified objects, we also add regular objects for which the revision has
                        // not changed to this list, so that we do not perform unnecessary attribute fetches when
                        // adding objects polled from registrations.
                        watchedObjects.insert(object.Object);
                    }
                    relevantCypressWatchlist.erase(cypressObjectIt);
                } else if (!isPolledObject) {
                    // NB: We do not delete non-replicated queues/consumers that are found in the polled objects list.
                    // There is no way to distinguish deleted regular objects from non-watched replicated objects,
                    // so we would just add them back if we were to delete them.
                    YT_LOG_DEBUG(
                        "Object was not found in corresponding watchlist, scheduled to be removed (Object: %v)",
                        object.Object);
                    RowsToDelete_.AppendObjectKey(object);

                    // Deleted replicated objects should also be removed from the replicated table mapping table.
                    if (IsReplicatedTableObjectType(object.ObjectType)) {
                        YT_LOG_DEBUG(
                            "Object scheduled to be removed from replicated table mapping table (Object: %v)",
                            object.Object);
                        RowsToDelete_.AppendReplicatedObjectKey(object);
                    }
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
                watchedObjects.insert(objectRef);
            }
        }

        // Add objects polled from registration table that were not added to the modified object list in the loops above.
        for (const auto& [objectPath, object] : ClusterToPolledObjects_[cluster]) {
            // NB: This will add regular queues/consumers, which are actually deleted.
            // There is no way to distinguish these objects from unwatched replicated objects.
            // NB: This will add watched objects with unchanged master revisions.
            // There is no way to distinguish these objects from watched replicated objects for which the master
            // revision doesn't always change when we want it to change (e.g. when the replica set is updated).
            // TODO(achulkov2): How long do we want to keep this behavior? Especially the latter.
            if (!watchedObjects.contains(object.Object)) {
                YT_LOG_DEBUG("Discovered polled registration table object (Object: %v)", object.Object);
                ClusterToModifiedObjects_[cluster].push_back(object);
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
                queue.ObjectType,
                queue.Revision,
                queue.RowRevision});
        }
        for (const auto& consumer : asyncConsumers.Get().Value()) {
            ClusterToDynamicStateObjects_[consumer.Ref.Cluster].push_back({
                consumer.Ref,
                ECypressSyncObjectType::Consumer,
                consumer.ObjectType,
                consumer.Revision,
                consumer.RowRevision});
        }
    }

    void FetchObjectsFromRegistrationTable()
    {
        auto registrations = WaitFor(DynamicState_->Registrations->Select())
            .ValueOrThrow();

        for (const auto& registration : registrations) {
            ClusterToPolledObjects_.Insert({
                .Object = registration.Queue,
                .Type = ECypressSyncObjectType::Queue,
            });
            ClusterToPolledObjects_.Insert({
                .Object = registration.Consumer,
                .Type = ECypressSyncObjectType::Consumer,
            });
        }

        for (const auto& [cluster, polledObjects] : ClusterToPolledObjects_) {
            YT_LOG_DEBUG("Fetched polled objects (Cluster: %v, Count: %v)", cluster, polledObjects.size());
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
        std::vector<std::string> clusters;
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

        for (int index = 0; index < std::ssize(combinedResults); ++index) {
            const auto& batchRsp = combinedResults[index];
            const auto& cluster = clusters[index];
            if (!batchRsp.IsOK()) {
                AlertCollector_->StageAlert(CreateAlert(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchAttributes,
                    "Error fetching attributes from clusters",
                    /*tags*/ {{"cluster", cluster}},
                    GetCumulativeError(batchRsp)));
                continue;
            }
            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToModifiedObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    YT_LOG_ERROR(
                        responseOrError,
                        "Error fetching attributes for object (Object: %v)",
                        object.Object);
                    RowsWithErrors_.AppendObjectWithError(object, responseOrError);
                    continue;
                }
                auto attributes = ConvertToAttributes(TYsonString(responseOrError.Value()->value()));
                YT_LOG_DEBUG(
                    "Fetched updated attributes (Object: %v, Attributes: %v)",
                    object.Object,
                    ConvertToYsonString(attributes, EYsonFormat::Text));

                // First, we try to interpret the attributes as one of two types: a queue, or a consumer.
                // If successful, we prepare an updated row for the corresponding state table.
                try {
                    RowsToWrite_.AppendObject(
                        object,
                        attributes,
                        DynamicConfigSnapshot_->ChaosReplicatedTableQueueAgentStage);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing object attributes (Object: %v)",
                        object.Object);
                    RowsWithErrors_.AppendObjectWithErrorAndBasicAttributes(
                        object,
                        attributes,
                        DynamicConfigSnapshot_->ChaosReplicatedTableQueueAgentStage,
                        ex,
                        Logger);
                }

                if (!DynamicConfigSnapshot_->WriteReplicatedTableMapping) {
                    continue;
                }

                // Some objects might be replicated, so we need to export them to a separate state table.
                // In the end, these objects appear in one of the regular state tables,
                // as well as in the replicated table mapping table.
                // These tables contain different kinds of information.
                // NB: We only export synchronization errors for object that have a replicated table type, so there will
                // be no entries for deleted objects, or objects for which the type attribute value is unavailable.
                try {
                    RowsToWrite_.AppendPotentiallyReplicatedObject(object, attributes);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing replicated object attributes (Object: %v)",
                        object.Object);
                    RowsWithErrors_.AppendReplicatedObjectWithError(object, ex);
                }
            }
        }
    }

    //! Write rows to dynamic state.
    void WriteRows()
    {
        if (!RowsWithErrors_.Empty()) {
            YT_LOG_DEBUG(
                "Some rows contain synchronization errors (QueueCount: %v, ConsumerCount: %v, ReplicatedTableMappingRowCount: %v)",
                RowsWithErrors_.QueueRows.size(),
                RowsWithErrors_.ConsumerRows.size(),
                RowsWithErrors_.ReplicatedTableMappingRows.size());
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

        if (!DynamicConfigSnapshot_->WriteReplicatedTableMapping) {
            YT_LOG_DEBUG("Writing replicated table mapping is disabled, skipping table modification");
            return;
        }

        YT_LOG_DEBUG(
            "Writing updated replicated table mapping table rows (Count: %v)",
            RowsToWrite_.ReplicatedTableMappingRows.size());
        WaitFor(DynamicState_->ReplicatedTableMapping->Insert(RowsToWrite_.ReplicatedTableMappingRows).AsVoid())
            .ThrowOnError();
    }

    //! Delete key rows from dynamic state.
    void DeleteRows()
    {
        YT_LOG_DEBUG(
            "Deleting rows (QueueCount: %v, ConsumerCount: %v)",
            RowsToDelete_.QueueRows.size(),
            RowsToDelete_.ConsumerRows.size());
        if (!RowsToDelete_.QueueRows.empty() || !RowsToDelete_.ConsumerRows.empty()) {
            WaitFor(AllSucceeded(std::vector{
                DynamicState_->Queues->Delete(RowsToDelete_.QueueRows),
                DynamicState_->Consumers->Delete(RowsToDelete_.ConsumerRows)}))
                .ThrowOnError();
        }

        if (!DynamicConfigSnapshot_->WriteReplicatedTableMapping) {
            YT_LOG_DEBUG("Writing replicated table mapping is disabled, skipping table outdated row deletion");
            return;
        }

        YT_LOG_DEBUG(
            "Deleting replicated table mapping rows (Count: %v)",
            RowsToDelete_.ReplicatedTableMappingRows.size());
        if (!RowsToDelete_.ReplicatedTableMappingRows.empty()) {
            WaitFor(DynamicState_->ReplicatedTableMapping->Delete(RowsToDelete_.ReplicatedTableMappingRows).AsVoid())
                .ThrowOnError();
        }
    }

    NNative::IClientPtr GetNativeClientOrThrow(const std::string& cluster) const
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
        TClientDirectoryPtr clientDirectory,
        IAlertCollectorPtr alertCollector)
        : Config_(std::move(config))
        , DynamicConfig_(New<TCypressSynchronizerDynamicConfig>())
        , ControlInvoker_(std::move(controlInvoker))
        , DynamicState_(std::move(dynamicState))
        , ClientDirectory_(std::move(clientDirectory))
        , AlertCollector_(std::move(alertCollector))
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
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("CypressSynchronizer"));

        auto finalizePass = Finally([&] {
            AlertCollector_->PublishAlerts();
        });

        if (!DynamicConfig_->Enable) {
            YT_LOG_DEBUG("Pass skipped");
            return;
        }

        PassInstant_ = TInstant::Now();
        ++PassIndex_;

        auto dynamicConfigSnapshot = CloneYsonStruct(DynamicConfig_);

        YT_LOG_DEBUG("Pass started (PassIndex: %v)", PassIndex_);
        try {
            TCypressSynchronizerPassSession(
                // NB: We need to make a copy so that the config does not change during context-switches within the pass session.
                dynamicConfigSnapshot,
                DynamicState_,
                ClientDirectory_,
                AlertCollector_,
                Logger().WithTag("PassIndex: %v", PassIndex_))
                .Build();
            PassError_ = TError();
        } catch (const std::exception& ex) {
            PassError_ = TError(ex);
            AlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::CypressSynchronizerPassFailed,
                "Error performing Cypress synchronizer pass",
                /*tags*/ {},
                ex));
        }

        YT_LOG_DEBUG("Pass finished (PassIndex: %v)", PassIndex_);
    }

    void OnDynamicConfigChanged(
        const TCypressSynchronizerDynamicConfigPtr& oldConfig,
        const TCypressSynchronizerDynamicConfigPtr& newConfig) override
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        DynamicConfig_ = newConfig;

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

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
    const IAlertCollectorPtr AlertCollector_;
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

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        BuildYsonFluently(consumer).BeginMap()
            .Item("active").Value(Active_)
            .Item("pass_instant").Value(PassInstant_)
            .Item("pass_index").Value(PassIndex_)
            .Item("pass_error").Value(PassError_)
        .EndMap();
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressSynchronizer)

ICypressSynchronizerPtr CreateCypressSynchronizer(
    TCypressSynchronizerConfigPtr config,
    IInvokerPtr controlInvoker,
    TDynamicStatePtr dynamicState,
    TClientDirectoryPtr clientDirectory,
    IAlertCollectorPtr alertCollector)
{
    return New<TCypressSynchronizer>(
        std::move(config),
        std::move(controlInvoker),
        std::move(dynamicState),
        std::move(clientDirectory),
        std::move(alertCollector));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
