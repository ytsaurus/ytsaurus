#include "cypress_synchronizer.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/iterator/zip.h>

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

constinit const auto Logger = CypressSynchronizerLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECypressSyncObjectKind,
    (Queue)
    (Consumer)
    (Unknown)
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
    TYPath Path;
    ECypressSyncObjectKind Kind;
    //! Master object type.
    std::optional<EObjectType> Type;
    //! The revision of the corresponding Cypress node.
    std::optional<NHydra::TRevision> Revision;
    //! The internal revision of the corresponding dynamic state row.
    std::optional<TRowRevision> RowRevision;
    bool HasReplicatedTableMappingRow = false;
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
        if (DynamicConfigSnapshot_->Policy == ECypressSynchronizerPolicy::Polling && DynamicConfigSnapshot_->WriteReplicatedTableMapping) {
            THROW_ERROR_EXCEPTION("Cypress synchronizer cannot work with replicated objects in polling mode");
        }

        FetchObjectMaps();

        ListObjectChanges();

        FetchAttributes();
        DeleteRows();
        WriteRows();
    }

private:
    const TCypressSynchronizerDynamicConfigPtr DynamicConfigSnapshot_;
    const TDynamicStatePtr DynamicState_;
    const TClientDirectoryPtr ClientDirectory_;
    const IAlertCollectorPtr AlertCollector_;
    const NLogging::TLogger Logger;

    using TClusterToObjectListMapping = THashMap<std::string, std::vector<TObject>>;

    class TClusterObjectRefMap
        : public THashMap<TYPath, const TObject*>
    {
    public:
        using THashMap::THashMap;

        auto InsertObject(const TObject* object)
        {
            return this->insert(std::pair{object->Path, object});
        }

        void InsertObjectOrCrash(const TObject* object)
        {
            InsertOrCrash(*this, std::pair{object->Path, object});
        }
    };

    // Represents list of objects from the same cluster.
    struct TClusterObjectList
    {
        std::string Cluster;
        std::vector<TObject> Objects;

        i64 AppendObject(TObject object, std::optional<ECypressSyncObjectKind> kindOverride = {})
        {
            if (kindOverride) {
                object.Kind = *kindOverride;
            }
            i64 index = std::ssize(Objects);
            Objects.push_back(std::move(object));
            return index;
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
        void AppendObject(const std::string& cluster, const TObject& object, const IAttributeDictionaryPtr& attributes, const std::string& chaosReplicatedTableQueueAgentStage)
        {
            auto fillChaosReplicatedTableQueueAgentStage = [=] (auto& row) {
                if (row.ObjectType && *row.ObjectType == EObjectType::ChaosReplicatedTable && !row.QueueAgentStage) {
                    row.QueueAgentStage = chaosReplicatedTableQueueAgentStage;
                }
            };

            switch (object.Kind) {
                case ECypressSyncObjectKind::Queue:
                    QueueRows.push_back(TQueueTableRow::FromAttributeDictionary(
                        TCrossClusterReference{cluster, object.Path},
                        NextRowRevision(object.RowRevision),
                        attributes));
                    fillChaosReplicatedTableQueueAgentStage(QueueRows.back());
                    break;
                case ECypressSyncObjectKind::Consumer:
                    ConsumerRows.push_back(TConsumerTableRow::FromAttributeDictionary(
                        TCrossClusterReference{cluster, object.Path},
                        NextRowRevision(object.RowRevision),
                        attributes));
                    fillChaosReplicatedTableQueueAgentStage(ConsumerRows.back());
                    break;
                case ECypressSyncObjectKind::Unknown:
                    // NB(apachee): Cypress sync object kind must be known at this point.
                    YT_ABORT();
            }
        }

        // NB: Must provide a strong exception-safety guarantee.
        void AppendPotentiallyReplicatedObject(const std::string& cluster, const TObject& object, const IAttributeDictionaryPtr& attributes) {
            auto tableRow =
                TReplicatedTableMappingTableRow::FromAttributeDictionary(TCrossClusterReference{cluster, object.Path}, attributes);
            const auto& type = tableRow.ObjectType;
            if (IsReplicatedTableObjectType(type)) {
                ReplicatedTableMappingRows.push_back(std::move(tableRow));
            }
        }

        void AppendObjectWithError(
            const std::string& cluster,
            const TObject& object,
            const TError& error,
            std::optional<NHydra::TRevision> revision = std::nullopt)
        {
            switch (object.Kind) {
                case ECypressSyncObjectKind::Queue:
                    QueueRows.push_back({
                        .Ref = TCrossClusterReference{cluster, object.Path},
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .Revision = revision,
                        .SynchronizationError = error,
                    });
                    break;
                case ECypressSyncObjectKind::Consumer:
                    ConsumerRows.push_back({
                        .Ref = TCrossClusterReference{cluster, object.Path},
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .Revision = revision,
                        .SynchronizationError = error,
                    });
                    break;
                case ECypressSyncObjectKind::Unknown:
                    // NB(apachee): Cypress sync object kind must be known at this point.
                    YT_ABORT();
            }
        }

        void AppendObjectWithErrorAndBasicAttributes(
            const std::string& cluster,
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

            switch (object.Kind) {
                case ECypressSyncObjectKind::Queue:
                    QueueRows.push_back({
                        .Ref = TCrossClusterReference{cluster, object.Path},
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .SynchronizationError = error,
                    });
                    fillBasicFieldsFromAttributes(QueueRows.back());
                    break;
                case ECypressSyncObjectKind::Consumer:
                    ConsumerRows.push_back({
                        .Ref = TCrossClusterReference{cluster, object.Path},
                        .RowRevision = NextRowRevision(object.RowRevision),
                        .SynchronizationError = error,
                    });
                    fillBasicFieldsFromAttributes(ConsumerRows.back());
                    break;
                case ECypressSyncObjectKind::Unknown:
                    // NB(apachee): Cypress sync object kind must be known at this point.
                    YT_ABORT();
            }
        }

        void AppendReplicatedObjectWithError(
            const std::string& cluster,
            const TObject& object,
            const TError& error,
            std::optional<NHydra::TRevision> revision = std::nullopt)
        {
            ReplicatedTableMappingRows.push_back({
                .Ref = TCrossClusterReference{cluster, object.Path},
                .Revision = revision,
                .SynchronizationError = error,
            });
        }

        void AppendObjectKey(const std::string& cluster, const TObject& object)
        {
            switch (object.Kind) {
                case ECypressSyncObjectKind::Queue:
                    QueueRows.push_back({.Ref = TCrossClusterReference{cluster, object.Path}});
                    break;
                case ECypressSyncObjectKind::Consumer:
                    ConsumerRows.push_back({.Ref = TCrossClusterReference{cluster, object.Path}});
                    break;
                case ECypressSyncObjectKind::Unknown:
                    // NB(apachee): Cypress sync object kind must be known at this point.
                    YT_ABORT();
            }
        }

        void AppendReplicatedObjectKey(const std::string& cluster, const TObject& object)
        {
            ReplicatedTableMappingRows.push_back({.Ref = TCrossClusterReference{cluster, object.Path}});
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

    TClusterToObjectListMapping ClusterToDynamicStateObjects_;
    TClusterToObjectListMapping ClusterToReplicatedTableMappingObjects_;
    std::vector<TClusterObjectList> ModifiedObjects_;
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
                batchReq->AddRequest(TYPathProxy::Get(object.Path + "/@attribute_revision"));
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

            TClusterObjectList clusterModifiedObjectList{
                .Cluster = cluster,
            };

            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (int objectIndex = 0; objectIndex < std::ssize(responses); ++objectIndex) {
                const auto& responseOrError = responses[objectIndex];
                const auto& object = GetOrCrash(ClusterToDynamicStateObjects_, cluster)[objectIndex];
                if (!responseOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        responseOrError,
                        "Error fetching revision (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);
                    RowsWithErrors_.AppendObjectWithError(cluster, object, responseOrError);
                    continue;
                }

                std::optional<NHydra::TRevision> revision;
                try {
                    revision = ConvertTo<NHydra::TRevision>(TYsonString(responseOrError.Value()->value()));
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing revision (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);
                    RowsWithErrors_.AppendObjectWithError(cluster, object, ex);
                    continue;
                }
                if (!object.Revision || *object.Revision < *revision) {
                    YT_LOG_DEBUG(
                        "Object Cypress revision changed (Cluster: %v, Path: %v, Revision: %x -> %x)",
                        cluster,
                        object.Path,
                        object.Revision,
                        revision);
                    clusterModifiedObjectList.AppendObject(object);
                }
            }

            ModifiedObjects_.push_back(std::move(clusterModifiedObjectList));
        }
    }

    struct TCypressWatchlist
        : public TYsonStructLite
    {
        static constexpr std::array<ECypressSyncObjectKind, 2> ObjectKinds{
            ECypressSyncObjectKind::Queue,
            ECypressSyncObjectKind::Consumer,
        };

        THashMap<TYPath, NHydra::TRevision> Queues;
        THashMap<TYPath, NHydra::TRevision> Consumers;

        THashMap<TYPath, NHydra::TRevision>& ObjectsByKind(ECypressSyncObjectKind kind)
        {
            switch (kind) {
                case ECypressSyncObjectKind::Queue:
                    return Queues;
                case ECypressSyncObjectKind::Consumer:
                    return Consumers;
                case ECypressSyncObjectKind::Unknown:
                    YT_ABORT();
            }
        }

        std::optional<std::pair<NHydra::TRevision, ECypressSyncObjectKind>> FindObjectInfo(TYPath key) const
        {
            if (auto it = Queues.find(key); it != Queues.end()) {
                return {{it->second, ECypressSyncObjectKind::Queue}};
            }
            if (auto it = Consumers.find(key); it != Consumers.end()) {
                return {{it->second, ECypressSyncObjectKind::Consumer}};
            }
            return std::nullopt;
        }

        REGISTER_YSON_STRUCT_LITE(TCypressWatchlist);

        static void Register(TRegistrar registrar)
        {
            registrar.Parameter("queues", &TThis::Queues)
                .Default();
            registrar.Parameter("consumers", &TThis::Consumers)
                .Default();

            registrar.Postprocessor([] (TThis* watchlist) {
                // NB(apachee): Verify invariant that must hold for cypress watchlist.

                for (const auto& [path, _] : watchlist->Queues) {
                    THROW_ERROR_EXCEPTION_IF(
                        watchlist->Consumers.contains(path),
                        "Set of queues and consumers must not intersect, but they do (ObjectPath: %v)",
                        path);
                }

                for (const auto& [path, _] : watchlist->Consumers) {
                    THROW_ERROR_EXCEPTION_IF(
                        watchlist->Queues.contains(path),
                        "Set of queues and consumers must not intersect, but they do (ObjectPath: %v)",
                        path);
                }
            });
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
    //!
    //! For dynamic state (queues/consumers) calculate diff with Cypress watchlist, which is enough to detect all changes to queues/consumers tables.
    //! It is important that we detect object type changes for proper syncing of replicated table mapping and we do detect those since
    //! we can assume it would change attribute revision (with overwhelming probability).
    //!
    //! For replicated table mapping diff with cypress watchlist is not enough, since it does not provide object types.
    //! Here is the description of how we detect all changes for replicated table mapping:
    //!   - Deleted objects are detected using cypress watchlist.
    //!   - Objects, which type changed from replicated table to regular table, are detected after fetching attributes and deleted from replicated
    //!   table mapping.
    //!   - Modified and unchanged objects are detected by comparing revision with one from Cypress watchlist,
    //!   but currently all replicated table mapping rows are assumed as changed, since revision does not change with replica set change.
    //!   - Missing objects that were also missing from dynamic state are detected in dynamic state diff.
    //!   - Missing objects that are only missing from replicated table mapping are detected by inducing which replicated table mapping objects
    //!   must be present according to updated dynamic state, e.g. all dynamic state objects with replicated table type must also be a replicated table object.
    void InferChangesFromClusterWatchlist(const std::string& cluster, TCypressWatchlist cypressWatchlist)
    {
        TClusterObjectList clusterModifiedObjectList{
            .Cluster = cluster,
        };

        // Generic object already watched by queue agent. Used for discovering new objects.
        TClusterObjectRefMap watchedObjects;

        // Replicated table mapping object already watched by queue agent.
        // Used for adding missing replicated_table_mapping table rows.
        TClusterObjectRefMap watchedReplicatedTableMappingObjects;

        THashMap<TYPath, i64> modifiedObjectIndexes;

        auto addModifiedObject = [&] (const TObject& object, ECypressSyncObjectKind kindOverride) {
            auto [it, inserted] = modifiedObjectIndexes.insert(std::pair{object.Path, -1});
            if (inserted) {
                it->second = clusterModifiedObjectList.AppendObject(object, kindOverride);
            }

            YT_VERIFY(it->second >= 0);

            return it->second;
        };

        // Infer changes from generic dynamic state objects (objects from queues and consumers table).

        if (auto clusterToDynamicStateObjectsIt = ClusterToDynamicStateObjects_.find(cluster); clusterToDynamicStateObjectsIt != ClusterToDynamicStateObjects_.end()) {
            THashSet<TYPath> removedObjects;

            for (const auto& object : clusterToDynamicStateObjectsIt->second) {
                auto objectInfo = cypressWatchlist.FindObjectInfo(object.Path);
                if (!objectInfo) {
                    if (removedObjects.insert(object.Path).second) {
                        YT_LOG_DEBUG(
                            "Object was not found in corresponding watchlist, scheduled to be removed (Cluster: %v, Path: %v)",
                            cluster,
                            object.Path);
                        RowsToDelete_.AppendObjectKey(cluster, object);
                    }
                    continue;
                }

                if (auto inserted = watchedObjects.InsertObject(&object).second; Y_UNLIKELY(!inserted)) {
                    YT_LOG_WARNING("Duplicate object paths present in current dynamic state (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);

                    AlertCollector_->StageAlert(CreateAlert(
                        NAlerts::EErrorCode::CypressSynchronizerConflictingDynamicStateObjects,
                        "Duplicate object paths present in current dynamic state",
                        /*tags*/ {{"cluster", cluster}, {"path", object.Path}},
                        TError("Object %v on cluster %v present multiple times in current dynamic state", object.Path, cluster)));
                }

                auto [revision, kind] = *objectInfo;

                // NB(apachee): Replicated table attributes change is handled in other part below and here we only
                // care about revision change.
                // TODO(apachee): In future it might be beneficial to limit fetched attributes for replicated objects to only those
                // needed for replicated table mapping, as other attributes change results in revision change.
                if (!object.Revision || revision > *object.Revision) {
                    YT_LOG_DEBUG(
                        "Object Cypress revision changed (Cluster: %v, Path: %v, Revision: %x -> %x)",
                        cluster,
                        object.Path,
                        object.Revision,
                        revision);

                    addModifiedObject(object, kind);
                }
            }
        }

        // Infer changes from replicated dynamic state objects (objects from replicated_table_mapping table).

        // NB(apachee): Effectively skipped if WriteReplicatedTableMapping is disabled, since in this case replicated table mapping table rows are not fetched.
        if (auto clusterToReplicatedTableMappingObjectsIt = ClusterToReplicatedTableMappingObjects_.find(cluster);
            clusterToReplicatedTableMappingObjectsIt != ClusterToReplicatedTableMappingObjects_.end())
        {
            THashSet<TYPath> removedReplicatedObjects;

            for (const auto& object : clusterToReplicatedTableMappingObjectsIt->second) {
                auto objectInfo = cypressWatchlist.FindObjectInfo(object.Path);
                if (!objectInfo) {
                    InsertOrCrash(removedReplicatedObjects, object.Path);

                    YT_LOG_DEBUG(
                        "Replicated object was not found in corresponding watchlist, scheduled to be removed from replicated table mapping table (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);
                    RowsToDelete_.AppendReplicatedObjectKey(cluster, object);

                    continue;
                }

                watchedReplicatedTableMappingObjects.InsertObjectOrCrash(&object);

                auto [revision, kind] = *objectInfo;

                // NB(apachee): Currently attribute revision does not change on replica set change, thus we always assume attributes of replicated table object changed.

                YT_LOG_DEBUG(
                    "Replicated object Cypress revision assumed as changed (Cluster: %v, Path: %v, OldRevision: %x, NewRevision: %x)",
                    cluster,
                    object.Path,
                    object.Revision,
                    revision);

                auto index = addModifiedObject(object, kind);

                clusterModifiedObjectList.Objects[index].HasReplicatedTableMappingRow = true;
            }
        }

        int missingReplicatedTableMappingObjectCount = 0;

        // The remaining objects are not present in the current dynamic state, thus they are all new and modified.

        for (const auto& kind : TCypressWatchlist::ObjectKinds) {
            for (const auto& [objectPath, revision] : cypressWatchlist.ObjectsByKind(kind)) {
                auto watchedObjectsIt = watchedObjects.find(objectPath);

                bool isNewDynamicStateObject = (watchedObjectsIt == watchedObjects.end());
                bool isNewReplicatedTableMappingObject = DynamicConfigSnapshot_->WriteReplicatedTableMapping
                    && !isNewDynamicStateObject && IsReplicatedTableObjectType(watchedObjectsIt->second->Type)
                    && !watchedReplicatedTableMappingObjects.contains(objectPath);

                if (isNewDynamicStateObject) {
                    YT_LOG_DEBUG("Discovered object (Cluster: %v, Path: %v, Revision: %v)",
                        cluster,
                        objectPath,
                        revision);
                } else if (isNewReplicatedTableMappingObject) {
                    YT_LOG_DEBUG("Discovered missing replicated table mapping object (Cluster: %v, Path: %v, Revision: %v)",
                        cluster,
                        objectPath,
                        revision);
                    ++missingReplicatedTableMappingObjectCount;
                }

                if (isNewDynamicStateObject || isNewReplicatedTableMappingObject) {
                    addModifiedObject(
                        TObject{
                            .Path = objectPath,
                            .Revision = revision,
                        },
                        kind);
                }
            }
        }

        if (Y_UNLIKELY(missingReplicatedTableMappingObjectCount > 0)) {
            // NB(apachee): As a side effect if WriteReplicatedTableMapping was only just enabled, this would stage an alert.
            // TODO(apachee): Persist alerts in alert collector to be able to check this alert in tests.
            YT_LOG_WARNING("Found objects present in current dynamic state with replicated table object type, but missing in replicated table mapping table (ObjectCount: %v)",
                missingReplicatedTableMappingObjectCount);
            AlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::CypressSynchronizerMissingReplicatedTableMappingObjects,
                "Found objects present in current dynamic state with replicated table object type, but missing in replicated table mapping table",
                /*tags*/ {{"cluster", cluster}},
                TError("%v objects missing in replicated table mapping table", missingReplicatedTableMappingObjectCount)));
        }

        ModifiedObjects_.push_back(std::move(clusterModifiedObjectList));
    }

    //! List all objects that appear in the dynamic state.
    void FetchObjectMaps()
    {
        auto asyncQueues = DynamicState_->Queues->Select();
        auto asyncConsumers = DynamicState_->Consumers->Select();

        // NB(apachee): Initialize with set future in case write_replicated_table_mapping is false.
        auto asyncReplicatedTableMapping = DynamicConfigSnapshot_->WriteReplicatedTableMapping
            ? DynamicState_->ReplicatedTableMapping->Select()
            : MakeFuture<std::vector<TReplicatedTableMappingTableRow>>({});

        WaitFor(AllSucceeded(std::vector{asyncQueues.AsVoid(), asyncConsumers.AsVoid(), asyncReplicatedTableMapping.AsVoid()}))
            .ThrowOnError();

        for (const auto& queue : asyncQueues.Get().Value()) {
            ClusterToDynamicStateObjects_[queue.Ref.Cluster].push_back({
                queue.Ref.Path,
                /*kind*/ ECypressSyncObjectKind::Queue,
                queue.ObjectType,
                queue.Revision,
                queue.RowRevision});
        }

        for (const auto& consumer : asyncConsumers.Get().Value()) {
            ClusterToDynamicStateObjects_[consumer.Ref.Cluster].push_back({
                consumer.Ref.Path,
                /*kind*/ ECypressSyncObjectKind::Consumer,
                consumer.ObjectType,
                consumer.Revision,
                consumer.RowRevision});
        }

        if (!DynamicConfigSnapshot_->WriteReplicatedTableMapping) {
            return;
        }

        for (const auto& replicatedObject : asyncReplicatedTableMapping.Get().Value()) {
            ClusterToReplicatedTableMappingObjects_[replicatedObject.Ref.Cluster].push_back({
                replicatedObject.Ref.Path,
                /*kind*/ ECypressSyncObjectKind::Unknown,
                replicatedObject.ObjectType,
                replicatedObject.Revision,
                /*rowRevision*/ std::nullopt});
        }
    }

    static std::vector<TString> GetCypressAttributeNames(const TObject& object)
    {
        switch (object.Kind) {
            case ECypressSyncObjectKind::Consumer:
                return TConsumerTableRow::GetCypressAttributeNames();
            case ECypressSyncObjectKind::Queue:
                return TQueueTableRow::GetCypressAttributeNames();
            case ECypressSyncObjectKind::Unknown:
                // NB(apachee): Not a valid option.
                YT_ABORT();
        }
    }

    //! Fetch attributes for the specified objects and update the corresponding dynamic state rows.
    void FetchAttributes()
    {
        // Fetch attributes for modified objects via batch requests to each cluster.

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        for (const auto& [cluster, modifiedObjects] : ModifiedObjects_) {
            auto proxy = CreateObjectServiceReadProxy(
                GetNativeClientOrThrow(cluster),
                EMasterChannelKind::Follower);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& object : modifiedObjects) {
                auto req = TYPathProxy::Get(object.Path + "/@");
                ToProto(
                    req->mutable_attributes()->mutable_keys(),
                    GetCypressAttributeNames(object));
                batchReq->AddRequest(req);
            }
            asyncResults.push_back(batchReq->Invoke());
        }
        auto combinedResults = WaitFor(AllSet(asyncResults))
            .ValueOrThrow();

        // Create rows for the modified objects with new attribute values and an increased row revision.

        for (const auto& [batchRsp, clusterToModifiedObjectsItem] : Zip(combinedResults, ModifiedObjects_)) {
            const auto& [cluster, modifiedObjects] = clusterToModifiedObjectsItem;

            if (!batchRsp.IsOK()) {
                AlertCollector_->StageAlert(CreateAlert(
                    NAlerts::EErrorCode::CypressSynchronizerUnableToFetchAttributes,
                    "Error fetching attributes from clusters",
                    /*tags*/ {{"cluster", cluster}},
                    GetCumulativeError(batchRsp)));
                continue;
            }

            auto responses = batchRsp.Value()->GetResponses<TYPathProxy::TRspGet>();
            for (i64 objectIndex = 0; objectIndex < std::ssize(modifiedObjects); ++objectIndex) {
                const auto& object = modifiedObjects[objectIndex];
                const auto& responseOrError = responses[objectIndex];
                if (!responseOrError.IsOK()) {
                    YT_LOG_ERROR(
                        responseOrError,
                        "Error fetching attributes for object (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);
                    RowsWithErrors_.AppendObjectWithError(cluster, object, responseOrError);
                    continue;
                }
                auto attributes = ConvertToAttributes(TYsonString(responseOrError.Value()->value()));
                YT_LOG_DEBUG(
                    "Fetched updated attributes (Cluster: %v, Path: %v, Attributes: %v)",
                    cluster,
                    object.Path,
                    ConvertToYsonString(attributes, EYsonFormat::Text));

                // First, we try to interpret the attributes as one of two kinds: a queue, or a consumer.
                // If successful, we prepare an updated row for the corresponding state table.
                try {
                    RowsToWrite_.AppendObject(
                        cluster,
                        object,
                        attributes,
                        DynamicConfigSnapshot_->ChaosReplicatedTableQueueAgentStage);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing object attributes (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);
                    RowsWithErrors_.AppendObjectWithErrorAndBasicAttributes(
                        cluster,
                        object,
                        attributes,
                        DynamicConfigSnapshot_->ChaosReplicatedTableQueueAgentStage,
                        ex,
                        Logger);
                }

                if (!DynamicConfigSnapshot_->WriteReplicatedTableMapping) {
                    continue;
                }

                if (object.HasReplicatedTableMappingRow && !IsReplicatedTableObjectType(object.Type)) {
                    // NB(apachee): Object type changed (e.g. object was re-created) and now replicated_table_mapping table row should be deleted.
                    RowsToDelete_.AppendReplicatedObjectKey(cluster, object);
                }

                // Some objects might be replicated, so we need to export them to a separate state table.
                // In the end, these objects appear in one of the regular state tables,
                // as well as in the replicated table mapping table.
                // These tables contain different kinds of information.
                // NB: We only export synchronization errors for object that have a replicated table type, so there will
                // be no entries for deleted objects, or objects for which the type attribute value is unavailable.
                try {
                    RowsToWrite_.AppendPotentiallyReplicatedObject(cluster, object, attributes);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(
                        ex,
                        "Error parsing replicated object attributes (Cluster: %v, Path: %v)",
                        cluster,
                        object.Path);
                    RowsWithErrors_.AppendReplicatedObjectWithError(cluster, object, ex);
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
