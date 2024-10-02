#pragma once

#include "attribute_policy.h"
#include "build_tags.h"
#include "object.h"
#include "reference_attribute.h"

#include <yt/yt/orm/server/access_control/public.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>
#include <yt/yt/orm/library/attributes/yson_builder.h>

// TODO(babenko): replace with public
#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IQueryContext
    : public TObjectsHolder
{
    virtual ~IQueryContext() = default;

    virtual IObjectTypeHandler* GetTypeHandler() = 0;
    virtual NQueryClient::NAst::TExpressionPtr CreateFieldExpression(
        const TDBFieldRef& fieldRef,
        bool wrapEvaluated) = 0;
    virtual NQueryClient::NAst::TExpressionPtr GetFieldExpression(
        const TDBField* field,
        const NYPath::TYPath& = NYPath::TYPath()) = 0;
    virtual NQueryClient::NAst::TExpressionPtr GetAnnotationExpression(std::string_view name) = 0;
    virtual NQueryClient::NAst::TExpressionPtr GetScalarAttributeExpression(
        const TDBField* field,
        const NYPath::TYPath& path,
        EAttributeExpressionContext expressionContext,
        NTableClient::EValueType type) = 0;

    virtual NQueryClient::NAst::TExpressionPtr GetScalarTimestampExpression(
        const TDBField* field,
        const NYPath::TYPath& path) = 0;

    virtual const TScalarAttributeIndexDescriptor* GetIndexDescriptor() const = 0;
    virtual NQueryClient::NAst::TQuery* GetQuery() const = 0;
    virtual std::string Finalize() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class THistoryEnabledAttributeSchema
{
public:
    void Update(THistoryEnabledAttributeSchema schema);

    THistoryEnabledAttributeSchema& AddPath(NYPath::TYPath path);

    // NB! Should not schedule any additional reads here.
    // All required attribute values must preloaded externally.
    // If `IsStoreScheduled()` returned true, old values are guaranteed to be preloaded.
    template <class TTypedObject>
    THistoryEnabledAttributeSchema& SetValueFilter(
        std::function<bool(const TTypedObject*)> valueFilter);

    THistoryEnabledAttributeSchema& SetValueMutator(
        std::function<std::unique_ptr<NYson::IYsonConsumer>(NYson::IYsonConsumer*)> valueMutator);

    const std::set<NYPath::TYPath>& GetPaths() const;

    bool HasValueFilter() const;
    bool RunValueFilter(const TObject* object) const;

    bool HasValueMutator() const;
    std::unique_ptr<NYson::IYsonConsumer> RunValueMutator(NYson::IYsonConsumer* consumer) const;

private:
    //! Paths of the attribute relative to the current attribute schema.
    std::set<NYPath::TYPath> Paths_;

    //! Determines if the new value of the attribute should be stored.
    std::function<bool(const TObject*)> ValueFilter_;
    std::function<std::unique_ptr<NYson::IYsonConsumer>(NYson::IYsonConsumer*)> ValueMutator_;

    const TAttributeSchema* Owner_ = nullptr;

    friend TAttributeSchema;
    THistoryEnabledAttributeSchema& SetOwner(const TAttributeSchema* attributeSchema);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChangedGetterType,
    // No `ChangedGetter` provided.
    ((None)             (0))
    // Corresponding `StoreScheduledGetter` should be provided and
    // `ChangedGetter` is guaranteed to immediately return false
    // if `StoredScheduledGetter` returned false for provided object.
    ((Default)          (1))
    // Always return true.
    ((ConstantTrue)     (2))
    // Always return false.
    ((ConstantFalse)    (3))
    // The `ChangedGetter` with arbitrary implementation.
    // Enabling history for attribute with custom `ChangedGetter`
    // leads to additional preload read phase in case no `ValueFilter` provided.
    ((Custom)           (4))
);

DEFINE_ENUM(EValueGetterType,
    // No `ValueGetter` provided.
    ((None)             (0))
    // The `ValueGetter` with arbitrary implementation.
    // There are no available reading optimization for that type.
    ((Evaluator)        (1))
    // The value is not dependent on provided path.
    // Fetching nested paths could be skipped by optimizations.
    ((Default)          (2))
);

////////////////////////////////////////////////////////////////////////////////

class TAttributeSchema
    : private TNonCopyable
{
public:
    virtual ~TAttributeSchema() = default;

    const std::string& GetName() const;
    NYPath::TYPath GetPath() const;
    std::string FormatPathEtc() const;
    std::string FormatTypePath() const;

    const TCompositeAttributeSchema* GetParent() const;
    TCompositeAttributeSchema* GetParent();

    IObjectTypeHandler* GetTypeHandler() const;

    TCompositeAttributeSchema* TryAsComposite();
    const TCompositeAttributeSchema* TryAsComposite() const;
    TCompositeAttributeSchema* AsComposite();
    const TCompositeAttributeSchema* AsComposite() const;

    TScalarAttributeSchema* TryAsScalar();
    const TScalarAttributeSchema* TryAsScalar() const;
    TScalarAttributeSchema* AsScalar();
    const TScalarAttributeSchema* AsScalar() const;

    TAttributeSchema* SetComputed();
    bool IsComputed() const;

    TAttributeSchema* SetOpaque(bool emitEntity = true);
    bool IsOpaque() const;
    bool EmitOpaqueAsEntity() const;

    TAttributeSchema* SetOpaqueForHistory();
    bool IsOpaqueForHistory() const;

    TAttributeSchema* SetUpdatePolicy(EUpdatePolicy policy = EUpdatePolicy::Updatable, NYPath::TYPath path = "");
    std::optional<EUpdatePolicy> GetUpdatePolicy() const;
    bool IsOpaqueForUpdates() const;
    TAttributeSchema* SetUpdatable(NYPath::TYPath path = "");
    bool IsUpdatable(const NYPath::TYPath& path = "") const;
    bool IsSuperuserOnlyUpdatable(const NYPath::TYPath& path = "") const;

    bool IsControl() const;

    TAttributeSchema* SetExtensible();
    bool IsExtensible(NYPath::TYPathBuf path = "") const;

    NYson::TProtobufWriterOptions::TUnknownYsonFieldModeResolver BuildUnknownYsonFieldModeResolver() const;

    TAttributeSchema* SetMandatory(const NYPath::TYPath& path = "");
    const THashSet<NYPath::TYPath>& GetMandatory() const;

    TAttributeSchema* SetReadPermission(NAccessControl::TAccessControlPermissionValue permission);
    NAccessControl::TAccessControlPermissionValue GetReadPermission() const;

    bool IsAnnotationsAttribute() const;

    TAttributeSchema* SetControlAttribute();

    template <class TTypedObject>
    TAttributeSchema* AddUpdatePrehandler(std::function<void(
        TTransaction*,
        const TTypedObject*)> prehandler);
    void RunUpdatePrehandlers(
        TTransaction* transaction,
        const TObject* object) const;

    template <class TTypedObject>
    TAttributeSchema* AddUpdateHandler(std::function<void(
        TTransaction*,
        TTypedObject*)> handler);
    void RunUpdateHandlers(
        TTransaction* transaction,
        TObject* object,
        IUpdateContext* context) const;

    template <class TTypedObject>
    TAttributeSchema* AddValidator(std::function<void(TTransaction*, const TTypedObject*)> handler);
    void RunValidators(
        TTransaction* transaction,
        const TObject* object) const;

    template <class TTypedObject>
    TAttributeSchema* SetPreloader(std::function<void(const TTypedObject*)> preloader);
    bool HasPreloader() const;
    void RunPreloader(const TObject* object, const NYPath::TYPath& path = {}) const;

    using TPathValidator = std::function<void(
        const TAttributeSchema*,
        const NYPath::TYPath&)>;

    NQueryClient::NAst::TExpressionPtr RunListContainsExpressionBuilder(
        IQueryContext* context,
        NQueryClient::NAst::TLiteralExpressionPtr literalExpr,
        const NYPath::TYPath& path,
        EAttributeExpressionContext expressionContext) const;

    void RunPostInitializers();

    TAttributeSchema* EnableHistory(
        THistoryEnabledAttributeSchema schema = THistoryEnabledAttributeSchema());
    //! Returns all subattribute paths with enabled history.
    std::set<NYPath::TYPath> GetHistoryEnabledAttributePaths() const;
    //! Supplies consumer with all subattribute values with enabled history
    //! regardless of the filtering result and whether attribute was updated or not.
    //! Returns nullptr if there is no history enabled attribute.
    void GetHistoryEnabledAttributes(
        TTransaction* transaction,
        const TObject* object,
        NAttributes::IYsonBuilder* consumer) const;
    //! Returns true iff there is at least one history enabled attribute for store,
    //! i.e. updated (store scheduled) and accepted by the history filter.
    bool HasHistoryEnabledAttributeForStore(const TObject* object, bool enableVerboseLogging = false) const;
    //! Should be called before a #HasHistoryEnabledAttributeForStore call.
    void PreloadHistoryEnabledAttributes(const TObject* object) const;
    bool HasStoreScheduledHistoryAttributes(const TObject* object) const;
    bool IsHistoryEnabledFor(const NYPath::TYPath& suffix) const;
    bool ShouldStoreHistoryIndexedAttribute(const TObject* object, const NYPath::TYPath& suffix) const;

    TAttributeSchema* EnableMetaResponseAttribute(bool enable = true);
    void PreloadMetaResponseAttributes(const TObject* object) const;
    bool IsMetaResponseAttribute() const;

    void ForEachLeafAttribute(const TOnScalarAttribute& onAttribute) const;
    void ForEachAttribute(const TOnAttribute& onAttribute) const;
    void ForEachLeafAttributeMutable(const TOnMutableAttribute& onMutableAttribute);
    void ForEachAttributeMutable(const TOnMutableAttribute& onMutableAttribute);

    void SetView();
    bool IsView() const;
    NClient::NObjects::TObjectTypeValue GetViewObjectType() const;

    void ForEachVisibleLeafAttribute(const std::function<void(const TAttributeSchema*)>& callback) const;

protected:
    TAttributeSchema(
        IObjectTypeHandler* typeHandler,
        TObjectManager* objectManager,
        std::string name);

    static void SetParent(TAttributeSchema* schema, TCompositeAttributeSchema* parent);

    std::function<void(const TObject*, const NYPath::TYPath&)> Preloader_;
    std::vector<std::function<void(TTransaction*, const TObject*)>> Validators_;

    bool ForbidValidators_ = false;

    IObjectTypeHandler* const TypeHandler_;
    TObjectManager* const ObjectManager_;

private:
    friend class TScalarAttributeSchemaBuilder;

    const std::string Name_;

    TPropagateConst<TCompositeAttributeSchema*> Parent_ = nullptr;

    std::function<NQueryClient::NAst::TExpressionPtr(
        IQueryContext*,
        NQueryClient::NAst::TLiteralExpression*,
        const NYPath::TYPath& path,
        EAttributeExpressionContext)> ListContainsExpressionBuilder_;

    std::vector<std::function<void(TTransaction*, const TObject*)>> UpdatePrehandlers_;

    std::vector<std::function<void(TTransaction*, TObject*)>> UpdateHandlers_;

    std::vector<std::function<void()>> PostInitializers_;

    // NB! For simplicity supports at most one history enabled subattribute.
    std::optional<THistoryEnabledAttributeSchema> HistoryEnabledAttribute_;

    std::function<bool()> ValueFilteredChildrenValidator_;

    bool MetaResponseAttribute_ = false;

    bool Extensible_ = false;
    THashSet<NYPath::TYPath> MandatoryPaths_;
    std::vector<NYPath::TYPath> UpdatablePaths_;
    std::vector<NYPath::TYPath> SuperuserOnlyUpdatablePaths_;
    bool Annotations_ = false;
    bool Opaque_ = false;
    bool EmitOpaqueAsEntity_ = false;
    bool Computed_ = false;
    bool OpaqueForHistory_ = false;
    bool Control_ = false;
    bool View_ = false;
    std::optional<EUpdatePolicy> UpdatePolicy_;
    NAccessControl::TAccessControlPermissionValue ReadPermission_ = NAccessControl::TAccessControlPermissionValues::None;

    NClient::NObjects::TObjectTypeValue ViewObjectType_;

    using TConsumedSchemasVector = std::vector<std::pair<NYson::IYsonConsumer*, const TAttributeSchema*>>;

    void FillHistoryEnabledAttributePaths(std::set<NYPath::TYPath>* result) const;
    bool GetHistoryEnabledAttributesImpl(
        TTransaction* transaction,
        const TObject* object,
        NAttributes::IYsonBuilder* builder,
        bool hasHistoryEnabledParent) const;

    bool ShouldStoreHistoryAttributeImpl(
        const TObject* object, const std::set<NYPath::TYPath>& paths, bool fromIndex, bool verboseLogging) const;
    bool ShouldStoreHistoryEnabledAttribute(const TObject* object, bool verboseLogging) const;

    bool IsAttributeAcceptableForHistory(bool hasHistoryEnabledParentAttribute) const;

    bool HasExtensibleParent() const;

    void ValidateUpdatableCompositeAttributeNotSupported() const;

    bool ForEachAttributeImpl(
        const TOnAttribute& onAttribute,
        bool leafOnly) const;
    bool ForEachAttributeMutableImpl(
        const TOnMutableAttribute& onMutableAttribute,
        bool leafOnly);
    // If `true` or `false` returned skip subtree traversal returning that value.
    // Otherwise return OR combined subtree results.
    using TOnAttributeWithHistoryEnabledParent = std::function<std::optional<bool>(const TAttributeSchema*, bool)>;
    bool ForEachAttributeWithHistoryEnabledParentImpl(
        const TOnAttributeWithHistoryEnabledParent& onAttribute,
        bool hasHistoryEnabledParentAttribute = false) const;

    void RunUpdateHandlerImpl(
        TTransaction* transaction,
        TObject* object,
        size_t handlerIndex,
        IUpdateContext* context) const;

    template <class TTypedObject, class TTypedValue>
    void InitPolicyValidator(
        const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor,
        TIntrusivePtr<IAttributePolicy<TTypedValue>> policy);
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeAttributeSchema
    : public TAttributeSchema
{
    using TBase = TAttributeSchema;
    template <typename T>
    using TKeyToChild = THashMap<std::string, TPropagateConst<T*>, THash<std::string>, TEqualTo<>>;

public:
    TCompositeAttributeSchema(
        IObjectTypeHandler* typeHandler,
        TObjectManager* objectManager,
        std::string name);

    void AddChild(TAttributeSchema* child);
    TCompositeAttributeSchema* AddChildren(const std::vector<TAttributeSchema*>& children);

    void AddEtcChildField(TScalarAttributeSchema* etcChild, std::string_view field);

    TAttributeSchema* FindChild(std::string_view key);
    const TAttributeSchema* FindChild(std::string_view key) const;

    TAttributeSchema* GetChild(std::string_view key);
    const TAttributeSchema* GetChild(std::string_view key) const;

    TScalarAttributeSchema* FindEtcChildByFieldName(std::string_view key);
    const TScalarAttributeSchema* FindEtcChildByFieldName(std::string_view key) const;

    TScalarAttributeSchema* GetEtcChild(std::string_view name = {});
    const TScalarAttributeSchema* GetEtcChild(std::string_view name = {}) const;
    const TKeyToChild<TAttributeSchema>& KeyToChild() const;
    TKeyToChild<TAttributeSchema>& KeyToChild();
    const TCompactFlatMap<std::string, TScalarAttributeSchema*, 4>& NameToEtcChild() const;

    void ForEachImmediateChild(const std::function<void(const TAttributeSchema*)>& callback) const;

    TCompositeAttributeSchema* SetUpdateMode(ESetUpdateObjectMode mode);
    std::optional<ESetUpdateObjectMode> GetUpdateMode() const;

protected:
    TKeyToChild<TAttributeSchema> KeyToChild_;
    TCompactFlatMap<std::string, TScalarAttributeSchema*, 4> NameToEtcChild_;
    TKeyToChild<TScalarAttributeSchema> KeyToEtcChild_;

    std::optional<ESetUpdateObjectMode> UpdateMode_;
};

////////////////////////////////////////////////////////////////////////////////

class TMetaAttributeSchema
    : public TCompositeAttributeSchema
{
    using TBase = TAttributeSchema;

public:
    using TCompositeAttributeSchema::TCompositeAttributeSchema;

    void DisableEtcFieldToMetaResponse(std::string_view name);
    const std::vector<NYPath::TYPath>& GetDisabledEtcFieldPathsToMetaResponse() const;

private:
    std::vector<NYPath::TYPath> DisabledEtcFieldPathsToMetaResponse_;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(grigminakov): Move all corresponding parts from base class in YTORM-1017.
class TScalarAttributeSchema
    : public TAttributeSchema
{
    using TBase = TAttributeSchema;

public:
    TScalarAttributeSchema(
        IObjectTypeHandler* typeHandler,
        TObjectManager* objectManager,
        std::string name);

    TScalarAttributeSchema* SetEtc();
    bool IsEtc() const;

    TScalarAttributeSchema* SetConstantChangedGetter(bool result);
    template <class TTypedObject>
    TScalarAttributeSchema* SetChangedGetter(std::function<bool(
        const TTypedObject*, const NYPath::TYPath&)> changedGetter);
    bool HasChangedGetter() const;
    EChangedGetterType GetChangedGetterType() const;
    bool RunChangedGetter(
        const TObject* object,
        const NYPath::TYPath& path = "") const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetFilteredChangedGetter(std::function<bool(
        const TTypedObject*,
        const NYPath::TYPath&,
        std::function<bool(const NYPath::TYPath&)>)>);
    bool HasFilteredChangedGetter() const;
    EChangedGetterType GetFilteredChangedGetterType() const;
    bool RunFilteredChangedGetter(
        const TObject* object,
        const NYPath::TYPath& path = "",
        std::function<bool(const NYPath::TYPath&)> byPathFilter = nullptr) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetAttributeGetter(
        std::function<const TAttributeBase*(const TTypedObject*)> getter);

    template <class TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetTypedValueSetter(std::function<void(
        TTransaction*,
        TTypedObject*,
        const NYPath::TYPath&,
        const TTypedValue&,
        bool,
        std::optional<bool>,
        EAggregateMode,
        const TTransactionCallContext&)> setter);

    TScalarAttributeSchema* SetValueSetter(std::function<void(
        TTransaction*,
        TObject*,
        const NYPath::TYPath&,
        const NYTree::INodePtr&,
        bool,
        std::optional<bool>,
        EAggregateMode,
        const TTransactionCallContext&)> setter);

    bool HasValueSetter() const;
    void RunValueSetter(
        TTransaction* transaction,
        TObject* object,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& value,
        bool recursive,
        std::optional<bool> sharedWrite,
        EAggregateMode aggregateMode,
        const TTransactionCallContext& transactionCallContext) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetLocker(std::function<void(
        TTransaction*,
        TTypedObject*,
        const NYPath::TYPath&,
        NTableClient::ELockType)> remover);
    bool HasLocker() const;
    void RunLocker(
        TTransaction* transaction,
        TObject* object,
        const NYPath::TYPath& path,
        NTableClient::ELockType lockType) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetRemover(std::function<void(
        TTransaction*,
        TTypedObject*,
        const NYPath::TYPath&,
        bool)> remover);
    bool HasRemover() const;
    void RunRemover(
        TTransaction* transaction,
        TObject* object,
        const NYPath::TYPath& path,
        bool force) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetTimestampPregetter(std::function<void(
        const TTypedObject*,
        const NYPath::TYPath&)> timestampPregetter);
    bool HasTimestampPregetter() const;
    void RunTimestampPregetter(
        const TObject* object,
        const NYPath::TYPath& path) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetTimestampGetter(std::function<TTimestamp(
        const TTypedObject*,
        const NYPath::TYPath&)> timestampGetter);
    bool HasTimestampGetter() const;
    TTimestamp RunTimestampGetter(
        const TObject* object,
        const NYPath::TYPath& path) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetStoreScheduledGetter(std::function<bool(const TTypedObject*)> storeScheduledGetter);
    bool HasStoreScheduledGetter() const;
    bool RunStoreScheduledGetter(const TObject* object) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetUpdatePreloader(
        std::function<void(TTransaction*, const TTypedObject*, const TUpdateRequest&)> preupdater);
    bool HasUpdatePreloader() const;
    void RunUpdatePreloader(
        TTransaction* transaction,
        const TObject* object,
        const TUpdateRequest& request) const;

    template <std::derived_from<TAttributeBase> TAttribute>
    const TAttribute* GetAttribute(const TObject* object) const;

    template <std::derived_from<TAttributeBase> TAttribute>
    TAttribute* GetAttribute(TObject* object) const;

    template <class TTypedObject>
    TScalarAttributeSchema* SetValueGetter(std::function<void(
        TTransaction*,
        const TTypedObject*,
        NYson::IYsonConsumer*)> valueGetter);
    template <class TTypedObject>
    TScalarAttributeSchema* SetValueGetter(std::function<void(
        TTransaction*,
        const TTypedObject*,
        NYson::IYsonConsumer*,
        const NYPath::TYPath& path)> valueGetter);
    EValueGetterType GetValueGetterType() const;
    bool HasValueGetter() const;
    void RunValueGetter(
        TTransaction* transaction,
        const TObject* object,
        NYson::IYsonConsumer* consumer,
        const NYPath::TYPath& path = {}) const;

    TScalarAttributeSchema* SetDefaultValueGetter(
        std::function<NYTree::INodePtr()> defaultValueGetter);
    bool HasDefaultValueGetter() const;
    NYTree::INodePtr RunDefaultValueGetter() const;

    bool HasInitializer() const;
    void RunInitializer(
        TTransaction* transaction,
        TObject* object) const;

    using TFieldExpressionGetter = std::function<NQueryClient::NAst::TExpressionPtr(
        IQueryContext*,
        const NYPath::TYPath&,
        EAttributeExpressionContext)>;

    TScalarAttributeSchema* SetExpressionBuilder(std::function<NQueryClient::NAst::TExpressionPtr(
        IQueryContext*)> builder);
    TScalarAttributeSchema* SetExpressionBuilder(
        TPathValidator pathValidator,
        TFieldExpressionGetter fieldExpressionGetter);
    bool HasExpressionBuilder() const;
    NQueryClient::NAst::TExpressionPtr RunExpressionBuilder(
        IQueryContext* context,
        const NYPath::TYPath& path,
        EAttributeExpressionContext expressionContext) const;

    TScalarAttributeSchema* SetTimestampExpressionBuilder(
        std::function<NQueryClient::NAst::TExpressionPtr(IQueryContext*, const NYPath::TYPath&)> builder);
    bool HasTimestampExpressionBuilder() const;
    NQueryClient::NAst::TExpressionPtr RunTimestampExpressionBuilder(
        IQueryContext* context,
        const NYPath::TYPath& path) const;

    template <class TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetPolicy(
        const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor,
        TIntrusivePtr<IAttributePolicy<TTypedValue>> policy);

    TScalarAttributeSchema* SetNestedFieldsMap(std::vector<std::string> fields);
    const std::vector<std::string>& GetNestedFieldsMap() const;

    std::vector<const TTagSet*> CollectChangedTags(const TObject* object) const;
    void SetTagsIndex(std::vector<TTagsIndexEntry> index);

    template <class TTypedObject, class TTypedRequest, class TTypedResponse>
    TScalarAttributeSchema* SetMethod(std::function<void(
        TTransaction*,
        TTypedObject*,
        const NYPath::TYPath&,
        const TTypedRequest&,
        TTypedResponse*)> method);

    bool HasMethod() const;
    void RunMethod(
        TTransaction* transaction,
        TObject* object,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& value,
        NYson::IYsonConsumer* consumer) const;

    template <class TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetControl(std::function<void(
        TTransaction*,
        TTypedObject*,
        const TTypedValue&)> control);

    template <class TTypedObject, class TTypedValue>
    TScalarAttributeSchema* SetControlWithTransactionCallContext(std::function<void(
        TTransaction*,
        TTypedObject*,
        const TTypedValue&,
        const TTransactionCallContext&)> control);

    TScalarAttributeSchema* SetIdAttribute(const TDBField* field, TValueGetter valueGetter);
    TScalarAttributeSchema* SetParentIdAttribute(const TDBField* field, TValueGetter valueGetter);

    template <std::derived_from<TScalarAttributeDescriptorBase> TDescriptor = TScalarAttributeDescriptorBase>
    const TDescriptor* GetAttributeDescriptor() const;
    template <std::derived_from<TScalarAttributeDescriptorBase> TDescriptor = TScalarAttributeDescriptorBase>
    bool HasAttributeDescriptor() const;

    const TDBField* TryGetDBField() const;
    const TDBField* GetDBField() const;

    TAttributeSchema* SetCustomPolicyExpected();

    // This is for key fields only.
    // TODO(deep): Pull this into SetIdAttribute.
    // TODO(kmokrov): Make two different sets of policies for generators and validators https://st.yandex-team.ru/YTORM-684.
    template<typename TTypedValue>
    TScalarAttributeSchema* SetKeyFieldPolicy(TIntrusivePtr<IAttributePolicy<TTypedValue>> policy);
    void SetKeyFieldEvaluator(std::function<std::optional<TObjectKey::TKeyField>(NYTree::INodePtr)> evaluator);
    void ValidateKeyField(
        const TObjectKey::TKeyField& keyField,
        std::string_view overrideTitle = {}) const;
    std::optional<TObjectKey::TKeyField> TryEvaluateKeyField(NYTree::INodePtr meta) const;
    TObjectKey::TKeyField GenerateKeyField(TTransaction* transaction) const;

    TScalarAttributeSchema* SetAccessControlParentIdAttribute(const TDBField* field);

    void Validate() const;

    bool IsAggregated() const;

    void SetProtobufElement(NYson::TProtobufElement element);
    const NYson::TProtobufElement& GetProtobufElement() const;

    // Type for etc-field should be requested with GetEtcChild and suffix as path directly.
    EAttributeType GetType(NYPath::TYPathBuf path = {}) const;

private:
    friend class TScalarAttributeSchemaBuilder;

    bool Etc_ = false;

    bool Aggregated_ = false;

    NYson::TProtobufElement ProtobufElement_;

    EChangedGetterType ChangedGetterType_ = EChangedGetterType::None;
    std::function<bool(const TObject*, const NYPath::TYPath&)> ChangedGetter_;

    EChangedGetterType FilteredChangedGetterType_ = EChangedGetterType::None;
    std::function<bool(
        const TObject*,
        const NYPath::TYPath&,
        std::function<bool(const NYPath::TYPath&)>)> FilteredChangedGetter_;

    std::function<const TAttributeBase*(const TObject*)> AttributeGetter_;

    std::function<void(
        TTransaction*,
        TObject*,
        const NYPath::TYPath&,
        const NYTree::INodePtr&,
        bool,
        std::optional<bool>,
        EAggregateMode,
        const TTransactionCallContext&)> ValueSetter_;

    std::function<void(
        TTransaction*,
        TObject*,
        const NYPath::TYPath&,
        const NYTree::INodePtr&,
        NYson::IYsonConsumer*)> MethodEvaluator_;

    std::function<void(TTransaction*, TObject*, const NYPath::TYPath&, NTableClient::ELockType)> Locker_;

    std::function<void(TTransaction*, TObject*, const NYPath::TYPath&, bool)> Remover_;

    std::function<void(const TObject*, const NYPath::TYPath&)> TimestampPregetter_;
    std::function<TTimestamp(const TObject*, const NYPath::TYPath&)> TimestampGetter_;
    std::function<bool(const TObject*)> StoreScheduledGetter_;
    std::function<void(TTransaction*, const TObject*, const TUpdateRequest&)> UpdatePreloader_;
    EValueGetterType ValueGetterType_ = EValueGetterType::None;
    std::function<void(TTransaction*, const TObject*, NYson::IYsonConsumer*, const NYPath::TYPath&)> ValueGetter_;
    std::function<NYTree::INodePtr()> DefaultValueGetter_;
    std::function<void(TTransaction*, TObject*)> Initializer_;

    // Every expression builder must meet following requirement:
    // if path `x` is a prefix of path `y` then expression built for `x` should
    // also fetch data for `y`. Internal optimizations may drop queries for `y` in that case.
    // Otherwise consider making the attribute evaluated by adding ValueGetter.
    std::function<NQueryClient::NAst::TExpressionPtr(
        IQueryContext*,
        const NYPath::TYPath&,
        EAttributeExpressionContext)> ExpressionBuilder_;

    std::function<NQueryClient::NAst::TExpressionPtr(
        IQueryContext*,
        const NYPath::TYPath&)> TimestampExpressionBuilder_;

    std::vector<TTagsIndexEntry> TagsIndex_;

    std::vector<std::string> NestedFieldsMap_;

    const TDBField* Field_ = nullptr;
    const TScalarAttributeDescriptorBase* AttributeDescriptor_ = nullptr;

    bool CustomPolicyExpected_ = false;

    bool IsIdAttribute_ = false;
    std::function<std::optional<TObjectKey::TKeyField>(NYTree::INodePtr)> KeyFieldEvaluator_;
    std::function<void(const TObjectKey::TKeyField&, std::string_view)> KeyFieldValidator_;
    std::function<TObjectKey::TKeyField(TTransaction*)> KeyFieldGenerator_;

    template <class TTypedObject, class TTypedValue>
    void InitPolicyInitializer(
        const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor,
        TIntrusivePtr<IAttributePolicy<TTypedValue>> policy);
};

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeSchema::TFieldExpressionGetter MakeFieldExpressionGetter(const TDBField* field);

NYTree::INodePtr GetAttributeDefaultValue(const TAttributeSchema* schema);

EAttributeType ResolveAttributeTypeByProtobufElement(
    const NYson::TProtobufElement& element, NYPath::TYPathBuf path = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define ATTRIBUTE_SCHEMA_INL_H_
#include "attribute_schema-inl.h"
#undef ATTRIBUTE_SCHEMA_INL_H_
