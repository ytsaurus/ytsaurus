#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeHandler
{
    using TDBFields = NObjects::TDBFields;
    using TScalarAttributeSchemas = std::vector<const TScalarAttributeSchema*>;

    virtual ~IObjectTypeHandler() = default;

    virtual void Initialize() = 0;
    virtual void PostInitialize() = 0;
    virtual void Validate() const = 0;

    virtual NMaster::IBootstrap* GetBootstrap() const = 0;

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() const = 0;
    virtual const TDBTable* GetTable() const = 0;
    // Table schema from Cypress obtained at ORM master start.
    virtual NTableClient::TTableSchemaPtr GetTableSchema() const = 0;
    virtual const TDBTable* GetParentsTable() const = 0;
    virtual const TDBFields& GetKeyFields() const = 0;
    virtual TObjectTypeValue GetType() const = 0;

    // Null keys (commonly, the string "" or the number 0 or -1) may not be used in live objects
    // but may be used to (in-band) indicate an unset reference. An empty null key means there is no
    // null key for this type.
    virtual TObjectKey GetNullKey() const = 0;

    // The key to look up the object's row in the DBTable.
    virtual TObjectKey GetObjectTableKey(
        const TObject* object,
        std::source_location location = std::source_location::current()) const = 0;
    // Returns (objectKey, parentKey or {}).
    virtual std::pair<TObjectKey, TObjectKey> SplitObjectTableKey(TObjectKey tableKey) const = 0;

    virtual bool HasParent() const = 0;
    virtual TObjectTypeValue GetParentType() const = 0;
    virtual int GetAncestryDepth() const = 0;
    virtual TObject* GetParent(
        const TObject* object,
        std::source_location location = std::source_location::current()) = 0;
    virtual const TDBFields& GetParentKeyFields() const = 0;
    virtual const TChildrenAttributeBase* GetParentChildrenAttribute(const TObject* parent) const = 0;
    virtual const TChildrenAttributeBase* GetChildrenAttribute(
        const TObject* object, TObjectTypeValue childType) const = 0;
    virtual void ForEachChildrenAttribute(
        const TObject* object, std::function<void(const TChildrenAttributeBase*)> onChildAttribute) const = 0;

    virtual bool HasNonTrivialAccessControlParent() const = 0;
    virtual TObjectTypeValue GetAccessControlParentType() const = 0;
    virtual void ScheduleAccessControlParentKeyLoad(const TObject* object) = 0;
    virtual TObject* GetAccessControlParent(
        const TObject* object,
        std::source_location location = std::source_location::current()) = 0;
    virtual const TDBFields& GetAccessControlParentKeyFields() = 0;

    virtual TObjectId GetSchemaObjectId() = 0;
    virtual TObject* GetSchemaObject(const TObject* object) = 0;

    virtual TResolveAttributeResult GetUuidLocation() const = 0;

    virtual const TCompositeAttributeSchema* GetRootAttributeSchema() const = 0;
    virtual const TMetaAttributeSchema* GetMetaAttributeSchema() const = 0;
    virtual const TScalarAttributeSchemas& GetIdAttributeSchemas() const = 0;
    virtual const TScalarAttributeSchemas& GetParentIdAttributeSchemas() const = 0;
    virtual const TScalarAttributeSchemas& GetAccessControlParentIdAttributeSchemas() const = 0;

    virtual const TScalarAttributeIndexDescriptor* GetIndexDescriptorOrThrow(const TString& name) = 0;
    virtual const std::vector<const TScalarAttributeIndexDescriptor*> GetIndexes() = 0;

    virtual NQuery::IFilterMatcherPtr GetHistoryFilter() const = 0;
    virtual bool HasHistoryEnabledAttributes() const = 0;
    virtual const std::set<NYPath::TYPath>& GetHistoryEnabledAttributePaths() const = 0;
    virtual const std::set<NYPath::TYPath>& GetHistoryIndexedAttributes() const = 0;
    virtual bool IsPathAllowedForHistoryFilter(const TString& path) const = 0;
    virtual void PreloadHistoryEnabledAttributes(const TObject* object) = 0;
    virtual bool HasStoreScheduledHistoryAttributes(const TObject* object) const = 0;
    virtual bool HasHistoryEnabledAttributeForStore(const TObject* object) const = 0;

    virtual std::vector<TWatchLog> GetWatchLogs() const = 0;

    virtual bool SkipStoreWithoutChanges() const = 0;

    virtual bool IsObjectNameSupported() const = 0;
    virtual bool IsBuiltin(const TObject* object) const = 0;

    virtual bool ForceZeroKeyEvaluation() const = 0;

    virtual std::optional<bool> IsAttributeChanged(
        const TObject* object,
        const TResolveAttributeResult& resolveResult,
        std::function<bool(const TScalarAttributeSchema* schema)> bySchemaFilter = nullptr,
        std::function<bool(const NYPath::TYPath&)> byPathFilter = nullptr) = 0;

    virtual std::optional<std::vector<const TTagSet*>> CollectChangedAttributeTagSets(const TObject* object) = 0;

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectKey& key,
        const TObjectKey& parentKey,
        ISession* session) = 0;

    // NB: User payload is applied after this method.
    virtual void InitializeCreatedObject(TTransaction* transaction, TObject* object) = 0;
    virtual void FinishObjectCreation(TTransaction* transaction, TObject* object) = 0;
    // NB: Called on transaction commit.
    virtual void ValidateCreatedObject(TTransaction* transaction, TObject* object) = 0;

    virtual void PreloadObjectRemoval(TTransaction* transaction, const TObject* object, IUpdateContext* context) = 0;
    virtual void CheckObjectRemoval(TTransaction* transaction, const TObject* object) = 0;

    // Starts object removal. All the references are still intact.
    virtual void StartObjectRemoval(TTransaction* transaction, TObject* object) = 0;
    // Gets called after object removal/finalization is finished.
    virtual void FinishObjectRemoval(TTransaction* transaction, TObject* object) = 0;

    virtual bool AreTagsEnabled() const = 0;

    virtual bool IsParentRemovalForbidden() const = 0;

    virtual void ProfileAttributes(TTransaction* transaction, const TObject* object) = 0;

    virtual void DoHandleRevisionTrackerUpdate(
        TObject* object,
        const NYPath::TYPath& trackerPath) = 0;

    virtual void ScheduleRevisionTrackerUpdate(
        TObject* object,
        TTransaction* transaction,
        const NYPath::TYPath& trackerPath) = 0;

    virtual void DoPrepareAttributeMigrations(
        TObject* object,
        const TBitSet<int>& attributeMigrations,
        const TBitSet<int>& forcedAttributeMigrations) = 0;

    virtual void DoFinalizeAttributeMigrations(
        TObject* object,
        const TBitSet<int>& attributeMigrations,
        const TBitSet<int>& forcedAttributeMigrations) = 0;

protected:
    friend TResolveAttributeResult ResolveAttribute(
        const IObjectTypeHandler* typeHandler,
        const NYPath::TYPath& path,
        TAttributeSchemaCallback callback,
        bool validateProtoSchemaCompliance);

    struct TResolveAttributeMutableResult
    {
        TAttributeSchema* Attribute;
        NYPath::TYPath SuffixPath;
    };

    virtual TResolveAttributeMutableResult ResolveAttribute(
        const NYPath::TYPath& path,
        TAttributeSchemaCallback callback,
        bool validateProtoSchemaCompliance) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
