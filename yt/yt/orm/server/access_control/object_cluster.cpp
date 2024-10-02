#include "object_cluster.h"

#include "access_control_hierarchy.h"
#include "access_control_manager.h"
#include "config.h"
#include "data_model_interop.h"
#include "private.h"

#include <yt/yt/orm/server/objects/db_schema.h>
#include <yt/yt/orm/server/objects/object_table_reader.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/type_handler.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NOrm::NServer::NAccessControl {

using namespace NConcurrency;

namespace {

using TSelectResult = std::vector<std::unique_ptr<TSnapshotObject>>;

////////////////////////////////////////////////////////////////////////////////

TSelectResult ProcessStreamResult(
    NObjects::IObjectTypeHandler* typeHandler,
    const IDataModelInteropPtr& dataModelInterop,
    NObjects::IObjectTableReader::TStreamResult&& result)
{
    TSelectResult results;
    auto rowSet = result.Rowset->GetRows();
    results.reserve(rowSet.size());
    for (const auto& row : rowSet) {
        results.push_back(dataModelInterop->ParseObject(typeHandler, row));
    }
    return results;
}

TSelectResult CombineAsyncParseResults(std::vector<TFuture<TSelectResult>> asyncResults)
{
    TSelectResult parsedObjects;
    for (auto& asyncResult : asyncResults) {
        auto parseResult = WaitForUnique(std::move(asyncResult))
            .ValueOrThrow();

        parsedObjects.reserve(parsedObjects.size() + parseResult.size());
        for (auto& parsedObject : parseResult) {
            parsedObjects.push_back(std::move(parsedObject));
        }
    }
    return parsedObjects;
}

TSelectResult SelectObjectsWithTableReader(
    const IDataModelInteropPtr& dataModelInterop,
    const NObjects::TTransactionPtr& transaction,
    TObjectTypeValue type)
{
    auto* typeHandler = transaction->GetSession()->GetTypeHandlerOrCrash(type);

    std::vector<TString> fieldNames;
    for (const auto* field : dataModelInterop->GetObjectQueryFields(typeHandler)) {
        fieldNames.push_back(field->Name);
    }

    auto objectTableReader = CreateObjectTableReader(
        transaction->GetBootstrap(),
        transaction->GetBootstrap()
            ->GetAccessControlManager()
            ->GetConfig()
            ->ObjectTableReader,
        typeHandler->GetTable(),
        std::move(fieldNames),
        NObjects::ObjectsTable.Fields.MetaRemovalTime.Name);

    auto futures = objectTableReader->StreamAll(
        transaction->GetStartTimestamp(),
        transaction->GetBootstrap()
            ->GetTransactionManager(),
        transaction->GetBootstrap()
            ->GetWorkerPoolInvoker("root", "access_object_snapshot"));

    std::vector<TFuture<TSelectResult>> asyncResults;
    asyncResults.reserve(futures.size());
    for (auto& future : futures) {
        asyncResults.push_back(future.ApplyUnique(BIND(
            &ProcessStreamResult,
            typeHandler,
            dataModelInterop)));
    }

    return CombineAsyncParseResults(std::move(asyncResults));
}

TSelectResult SelectObjects(
    const IDataModelInteropPtr& dataModelInterop,
    const NObjects::TTransactionPtr& transaction,
    TObjectTypeValue type)
{
    if (transaction->GetBootstrap()
        ->GetAccessControlManager()
        ->GetConfig()
        ->EnableObjectTableReader)
    {
        return SelectObjectsWithTableReader(dataModelInterop, transaction, type);
    }

    auto* typeHandler = transaction->GetSession()->GetTypeHandlerOrCrash(type);
    auto rowset = transaction->SelectFields(
        type,
        dataModelInterop->GetObjectQueryFields(typeHandler));
    auto rows = rowset->GetRows();
    TSelectResult result;
    result.reserve(rows.Size());
    for (auto row : rows) {
        result.push_back(dataModelInterop->ParseObject(
            typeHandler,
            row));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TSnapshotObject::TSnapshotObject(
    TObjectTypeValue type,
    TObjectKey key,
    TObjectKey accessControlParentKey,
    TAccessControlList acl,
    bool inheritAcl,
    NYson::TYsonString labels)
    : Type_(type)
    , Key_(std::move(key))
    , AccessControlParentKey_(std::move(accessControlParentKey))
    , Acl_(std::move(acl))
    , InheritAcl_(inheritAcl)
    , Labels_(std::move(labels))
{ }

////////////////////////////////////////////////////////////////////////////////

void TClusterObjectSnapshot::AddObjects(
    TObjectTypeValue objectType,
    std::vector<std::unique_ptr<TSnapshotObject>> objects)
{
    EmplaceOrCrash(ObjectTypes_, objectType);
    for (auto& object : objects) {
        // NB! It is crucial to construct this argument in a separate code line
        //     to overcome UB due to unspecified order between
        //     TObject::Key call and std::unique_ptr<T> move constructor.
        auto key = object->Key();
        EmplaceOrCrash(Objects_[objectType], std::move(key), std::move(object));
    }
    SortedObjects_[objectType].reserve(objects.size());
    for (const auto& keyAndObjectPair : Objects_[objectType]) {
        SortedObjects_[objectType].push_back(keyAndObjectPair.second.get());
    }
    std::sort(
        SortedObjects_[objectType].begin(),
        SortedObjects_[objectType].end(),
        [] (auto* lhs, auto* rhs) {
            return lhs->Key() < rhs->Key();
        });
}

TSnapshotObject* TClusterObjectSnapshot::FindObject(
    TObjectTypeValue type,
    const TObjectKey& key) const
{
    if (auto it = Objects_.find(type); it != Objects_.end()) {
        if (auto it2 = it->second.find(key); it2 != it->second.end()) {
            return it2->second.get();
        }
    }
    return nullptr;
}

TRange<TSnapshotObject*> TClusterObjectSnapshot::GetSortedObjects(
    TObjectTypeValue type) const
{
    if (auto it = SortedObjects_.find(type); it != SortedObjects_.end()) {
        return TRange<TSnapshotObject*>(it->second);
    } else {
        static const std::vector<TSnapshotObject*> EmptySortedObjects;
        return TRange<TSnapshotObject*>(EmptySortedObjects);
    }
}

bool TClusterObjectSnapshot::ContainsObjectType(
    TObjectTypeValue objectType) const
{
    return ObjectTypes_.find(objectType) != ObjectTypes_.end();
}

void TClusterObjectSnapshot::ValidateContainsObjectType(
    TObjectTypeValue objectType) const
{
    if (!ContainsObjectType(objectType)) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::NoSuchMethod,
            "Cluster access control object snapshot does not contain %Qv objects",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(objectType));
    }
}

////////////////////////////////////////////////////////////////////////////////

TClusterObjectSnapshotPtr BuildClusterObjectSnapshot(
    const IDataModelInteropPtr& dataModelInterop,
    const NObjects::TTransactionPtr& transaction,
    const std::vector<TObjectTypeValue>& leafObjectTypes)
{
    THashSet<TObjectTypeValue> visitedObjectTypes;
    {
        std::queue<TObjectTypeValue> objectTypes;
        auto tryPushObjectType = [&visitedObjectTypes, &objectTypes] (TObjectTypeValue objectType) {
            if (visitedObjectTypes.insert(objectType).second) {
                objectTypes.push(objectType);
            }
        };
        for (auto leafObjectType : leafObjectTypes) {
            if (leafObjectType != TObjectTypeValues::Null) {
                tryPushObjectType(leafObjectType);
            }
        }
        TTransactionAccessControlHierarchy hierarchy;
        while (!objectTypes.empty()) {
            auto objectType = objectTypes.front();
            objectTypes.pop();
            auto* typeHandler = transaction->GetSession()->GetTypeHandlerOrCrash(objectType);
            hierarchy.ForEachImmediateParentType(typeHandler, tryPushObjectType);
        }
    }

    struct TResult
    {
        TObjectTypeValue ObjectType;
        std::vector<std::unique_ptr<TSnapshotObject>> Objects;
    };

    std::vector<TFuture<std::shared_ptr<TResult>>> asyncResults;
    asyncResults.reserve(visitedObjectTypes.size());

    for (auto objectType : visitedObjectTypes) {
        asyncResults.push_back(
            BIND([=] {
                auto typeName = NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType);
                try{
                    YT_LOG_DEBUG("Started loading object snapshot (ObjectType: %v)",
                        typeName);
                    auto objects = SelectObjects(dataModelInterop, transaction, objectType);
                    auto result = std::make_shared<TResult>();
                    result->ObjectType = objectType;
                    result->Objects = std::move(objects);
                    YT_LOG_DEBUG("Finished loading object snapshot (ObjectType: %v, ObjectCount: %v)",
                        typeName,
                        result->Objects.size());
                    return result;
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Error loading object snapshot (ObjectType: %v)",
                        typeName);
                    throw;
                }
            }).AsyncVia(GetCurrentInvoker()).Run());
    }

    auto results = WaitFor(AllSucceeded(std::move(asyncResults)))
        .ValueOrThrow();

    auto snapshot = New<TClusterObjectSnapshot>();
    for (auto& result : results) {
        snapshot->AddObjects(result->ObjectType, std::move(result->Objects));
    }

    return snapshot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
