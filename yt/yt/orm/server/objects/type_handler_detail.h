#pragma once

#include "type_handler.h"

#include "attribute_profiler.h"

#include <yt/yt/orm/server/access_control/public.h>

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/client/api/public.h>

#include <set>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    TObjectTypeHandlerBase(
        NMaster::IBootstrap* bootstrap,
        TObjectTypeValue type);

    void Initialize() override;
    void PostInitialize() override;
    void Validate() const override final;

    NMaster::IBootstrap* GetBootstrap() const override;
    NTableClient::TTableSchemaPtr GetTableSchema() const final;
    TObjectTypeValue GetType() const final;

    TObjectKey GetNullKey() const override;

    // Standard implementation prefixing the parent key (if present) to the object's key.
    TObjectKey GetObjectTableKey(
        const TObject* object,
        std::source_location location = std::source_location::current()) const final;

    // Returns object key first, parent key (if present) second.
    std::pair<TObjectKey, TObjectKey> SplitObjectTableKey(TObjectKey tableKey) const final;

    bool HasParent() const final;
    TObjectTypeValue GetParentType() const override;
    int GetAncestryDepth() const final;
    TObject* GetParent(
        const TObject* object,
        std::source_location location = std::source_location::current()) override;
    const TDBFields& GetParentKeyFields() const override;
    const TDBTable* GetParentsTable() const override;

    const TChildrenAttributeBase* GetParentChildrenAttribute(const TObject* parent) const final;
    const TChildrenAttributeBase* GetChildrenAttribute(const TObject* object, TObjectTypeValue childType) const final;
    void ForEachChildrenAttribute(
        const TObject* object, std::function<void(const TChildrenAttributeBase*)> onChildAttribute) const final;

    bool HasNonTrivialAccessControlParent() const final;
    TObjectTypeValue GetAccessControlParentType() const override;
    void ScheduleAccessControlParentKeyLoad(const TObject* object) override;
    TObject* GetAccessControlParent(
        const TObject* object,
        std::source_location location = std::source_location::current()) override;
    const TDBFields& GetAccessControlParentKeyFields() override;

    TObjectId GetSchemaObjectId() final;
    TObject* GetSchemaObject(const TObject* object) final;

    TResolveAttributeResult GetUuidLocation() const override;

    const TCompositeAttributeSchema* GetRootAttributeSchema() const final;
    const TMetaAttributeSchema* GetMetaAttributeSchema() const final;
    const TScalarAttributeSchemas& GetIdAttributeSchemas() const final;
    const TScalarAttributeSchemas& GetParentIdAttributeSchemas() const final;
    const TScalarAttributeSchemas& GetAccessControlParentIdAttributeSchemas() const final;

    TResolveAttributeMutableResult ResolveAttribute(
        const NYPath::TYPath& path,
        TAttributeSchemaCallback callback,
        bool validateProtoSchemaCompliance) const final;

    const TScalarAttributeIndexDescriptor* GetIndexDescriptorOrThrow(const TString& name) final;
    const std::vector<const TScalarAttributeIndexDescriptor*> GetIndexes() final;

    NQuery::IFilterMatcherPtr GetHistoryFilter() const override;
    bool HasHistoryEnabledAttributes() const override;
    const std::set<NYPath::TYPath>& GetHistoryEnabledAttributePaths() const override;
    const std::set<NYPath::TYPath>& GetHistoryIndexedAttributes() const final;
    bool IsPathAllowedForHistoryFilter(const NYPath::TYPath& path) const override;
    void PreloadHistoryEnabledAttributes(const TObject* object) override;
    bool HasHistoryEnabledAttributeForStore(const TObject* object) const override;
    bool HasStoreScheduledHistoryAttributes(const TObject* object) const override;

    std::vector<TWatchLog> GetWatchLogs() const override;

    bool SkipStoreWithoutChanges() const override;

    bool IsObjectNameSupported() const override;
    bool IsBuiltin(const TObject* object) const override;

    bool ForceZeroKeyEvaluation() const override;

    std::optional<bool> IsAttributeChanged(
        const TObject* object,
        const TResolveAttributeResult& resolveResult,
        std::function<bool(const TScalarAttributeSchema* schema)> bySchemaFilter = nullptr,
        std::function<bool(const NYPath::TYPath&)> byPathFilter = nullptr) override;

    std::optional<std::vector<const TTagSet*>> CollectChangedAttributeTagSets(const TObject* object) override;

    void InitializeCreatedObject(TTransaction* transaction, TObject* object) override;
    void FinishObjectCreation(TTransaction* transaction, TObject* object) override;
    void ValidateCreatedObject(TTransaction* transaction, TObject* object) override;

    void PreloadObjectRemoval(TTransaction* transaction, const TObject* object, IUpdateContext* context) override;
    void CheckObjectRemoval(TTransaction* transaction, const TObject* object) override;
    void StartObjectRemoval(TTransaction* transaction, TObject* object) override;
    void FinishObjectRemoval(TTransaction* transaction, TObject* object) override;

    bool AreTagsEnabled() const override;

    bool IsParentRemovalForbidden() const final;

    void ProfileAttributes(TTransaction* transaction, const TObject* object) final;

    void DoHandleRevisionTrackerUpdate(
        TObject* object,
        const NYPath::TYPath& trackerPath) override;

    void ScheduleRevisionTrackerUpdate(
        TObject* object,
        TTransaction* transaction,
        const NYPath::TYPath& trackerPath) override;

    void DoPrepareAttributeMigrations(
        TObject* object,
        const TBitSet<int>& attributeMigrations,
        const TBitSet<int>& forcedAttributeMigrations) override;

    void DoFinalizeAttributeMigrations(
        TObject* object,
        const TBitSet<int>& attributeMigrations,
        const TBitSet<int>& forcedAttributeMigrations) override;

protected:
    struct THistoryAttribute
    {
        NYPath::TYPath Path;
        bool Indexed = false;
        bool AllowedInFilter = false;
    };

    struct TTrackerOptions
    {
        bool LockGroupRestrictionEnabled;
        THashSet<NYPath::TYPath> TrackedFields;
        THashSet<NYPath::TYPath> ExcludedFields;
    };

    NMaster::IBootstrap* const Bootstrap_;
    const TObjectTypeValue Type_;

    const TObjectId SchemaId_;

    std::optional<int> AncestryDepth_;
    std::vector<std::unique_ptr<TAttributeSchema>> AttributeSchemas_;
    TCompositeAttributeSchema* RootAttributeSchema_ = nullptr;
    TMetaAttributeSchema* MetaAttributeSchema_ = nullptr;
    TScalarAttributeSchema* LabelsAttributeSchema_ = nullptr;
    TCompositeAttributeSchema* SpecAttributeSchema_ = nullptr;
    TCompositeAttributeSchema* StatusAttributeSchema_ = nullptr;
    TScalarAttributeSchema* AnnotationsAttributeSchema_ = nullptr;
    TCompositeAttributeSchema* ControlAttributeSchema_ = nullptr;

    TScalarAttributeSchemas IdAttributeSchemas_;
    TScalarAttributeSchemas ParentIdAttributeSchemas_;
    TScalarAttributeSchemas AccessControlParentIdAttributeSchemas_;

    THashMap<NYPath::TYPath, TTrackerOptions> RevisionTrackerOptionsByPath_;

    NTableClient::TTableSchemaPtr TableSchema_;

    bool PostInitializeCalled_ = false;
    bool ForbidParentRemoval_ = false;

    TScalarAttributeSchema* MakeScalarAttributeSchema(const TString& name);

    template <typename TDescriptor>
    TScalarAttributeSchema* MakeScalarAttributeSchema(const TString& name, const TDescriptor& descriptor);

    template <typename TDescriptor>
    TScalarAttributeSchema* MakeEtcAttributeSchema(const TString& name, const TDescriptor& descriptor);

    template <typename TDescriptor>
    TScalarAttributeSchema* MakeEtcAttributeSchema(const TDescriptor& descriptor);

    template <class TTypedObject, class TTypedValue>
    TScalarAttributeSchema* MakeProtobufAttributeSchema(
        const TString& name,
        const TScalarAttributeDescriptor<TTypedObject, TString>& descriptor);

    TCompositeAttributeSchema* MakeCompositeAttributeSchema(const TString& name);
    TMetaAttributeSchema* MakeMetaAttributeSchema();

    TScalarAttributeSchema* MakeIdAttributeSchema(
        const TString& name,
        const TDBField* field,
        TValueGetter valueGetter);
    TScalarAttributeSchema* MakeParentIdAttributeSchema(
        const TString& name,
        const TDBField* field,
        TValueGetter valueGetter);

    template <typename TDescriptor>
    TScalarAttributeSchema* MakeAccessControlParentIdAttributeSchema(
        const TString& name,
        const TDescriptor& descriptor);

    virtual std::vector<NAccessControl::TAccessControlEntry> GetDefaultAcl();

    void RegisterScalarAttributeIndex(
        TString indexName, std::unique_ptr<TScalarAttributeIndexDescriptor> indexDescriptor);

    void RegisterChildrenAttribute(
        TObjectTypeValue objectType, std::function<const TChildrenAttributeBase*(const TObject*)> getter);

    // Only configures history paths and object filter.
    // Value filters / preloaders / mutators are configured separately via plugins if needed.
    void ConfigureHistory(
        std::vector<THistoryAttribute> attributes,
        std::optional<TObjectFilter> filter);
    virtual bool EnableVerboseHistoryLogging() const;

    void ValidateAcl(TTransaction* transaction, const TObject* object) const;

    void FillUpdatePolicies();

    virtual void OnConfigUpdate(const TObjectManagerConfigPtr& config);

    void AddAttributeSensor(const NYPath::TYPath& path, NClient::NProto::EAttributeSensorPolicy policy);
    void SetAttributeSensorValueTransform(const NYPath::TYPath& path, IAttributeProfiler::TValueTransform transform);

    void PrepareRevisionTracker(
        const THashSet<NYPath::TYPath>& trackedPaths,
        const NYPath::TYPath& trackerPath,
        bool lockGroupRestrictionEnabled,
        const THashSet<NYPath::TYPath>& excludedPaths = {});

    void PrepareExcludedFields(
        const NYPath::TYPath& trackerPath,
        const THashSet<NYPath::TYPath>& excludedPaths);

    bool NeedsRevisionUpdate(
        TObject* object,
        TScalarAttributeSchema* trackerSchema,
        const TResolveAttributeResult& trackedAttribute);

private:
    NQuery::IFilterMatcherPtr HistoryFilter_;
    bool HasHistoryEnabledAttributes_ = false;
    std::set<NYPath::TYPath> HistoryIndexedAttributePaths_;
    std::set<NYPath::TYPath> HistoryEnabledAttributePaths_;
    std::set<NYPath::TYPath> HistoryFilterAttributePaths_;

    THashMap<TString, std::unique_ptr<TScalarAttributeIndexDescriptor>> IndexDescriptors_;

    THashMap<NYPath::TYPath, IAttributeProfilerPtr> AttributeSensors_;

    THashMap<TObjectTypeValue, std::function<const TChildrenAttributeBase*(const TObject*)>> ChildrenAttributes_;

    void EnsureAncestryDepthInitialized();
    void PrepareHistoryEnabledAttributeSchemaCache();

    void EvaluateKey(
        const TTransaction* transaction,
        const TObject* object,
        NYson::IYsonConsumer* consumer) const;
    void EvaluateParentKey(
        const TTransaction* transaction,
        const TObject* object,
        NYson::IYsonConsumer* consumer) const;

    void PreloadFqid(const TObject* object) const;
    void EvaluateFqid(
        const TTransaction* transaction,
        const TObject* object,
        NYson::IYsonConsumer* consumer) const;

    void InitializeTags(TAttributeSchema* attributeSchema);

    bool IsEventGenerationSkipAllowed(TObjectTypeValue objectType);

    void SetupFinalizers();
    static void GetActiveFinalizers(TTransaction* transaction, const TObject* object, NYson::IYsonConsumer* consumer);
    static void HandleFinalizationStartTimeUpdate(TTransaction* transaction, TObject* object);
    static void HandleFinalizersUpdate(TTransaction* transaction, TObject* object);

    TScalarAttributeSchema* MakeAnnotationsAttributeSchema(const TString& name);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define TYPE_HANDLER_DETAIL_INL_H_
#include "type_handler_detail-inl.h"
#undef TYPE_HANDLER_DETAIL_INL_H_
