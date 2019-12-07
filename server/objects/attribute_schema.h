#pragma once

#include "object.h"
#include "transaction.h"

#include <yp/server/access_control/public.h>

// TODO(babenko): replace with public
#include <yt/ytlib/query_client/ast.h>

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributePath(const NYPath::TYPath& attributePath);

////////////////////////////////////////////////////////////////////////////////

struct IQueryContext
{
    virtual ~IQueryContext() = default;

    virtual IObjectTypeHandler* GetTypeHandler() = 0;
    virtual NYT::NQueryClient::NAst::TExpressionPtr GetFieldExpression(const TDBField* field) = 0;
    virtual NYT::NQueryClient::NAst::TExpressionPtr GetAnnotationExpression(const TString& name) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct THistoryEnabledAttributeSchema
{
    THistoryEnabledAttributeSchema& SetPath(NYPath::TYPath path);

    template <class TTypedObject>
    THistoryEnabledAttributeSchema& SetValueFilter(std::function<bool(TTypedObject*)> valueFilter);

    //! Path of the attribute relative to the current attribute schema.
    NYPath::TYPath Path;

    //! Determines if the new value of the attribute should be stored.
    std::function<bool(TObject*)> ValueFilter;
};

////////////////////////////////////////////////////////////////////////////////

class TAttributeSchema
    : private TNonCopyable
{
public:
    TAttributeSchema(
        IObjectTypeHandler* typeHandler,
        TObjectManager* objectManager,
        const TString& name);

    TAttributeSchema* SetOpaque();
    bool IsOpaque() const;

    bool IsControl() const;

    const TString& GetName() const;
    NYPath::TYPath GetPath() const;

    TAttributeSchema* GetParent() const;
    void SetParent(TAttributeSchema* parent);

    TAttributeSchema* SetComposite();
    bool IsComposite() const;

    TAttributeSchema* SetExtensible();
    bool IsExtensible() const;

    TAttributeSchema* SetMandatory();
    bool GetMandatory() const;

    TAttributeSchema* SetUpdatable();
    bool GetUpdatable() const;

    TAttributeSchema* SetEtc();
    bool IsEtc() const;

    TAttributeSchema* SetReadPermission(NAccessControl::EAccessControlPermission permission);
    NAccessControl::EAccessControlPermission GetReadPermission() const;

    void AddChild(TAttributeSchema* child);
    TAttributeSchema* AddChildren(const std::vector<TAttributeSchema*>& children);
    TAttributeSchema* FindChild(const TString& key) const;
    TAttributeSchema* FindEtcChild() const;
    TAttributeSchema* GetChildOrThrow(const TString& key) const;
    const THashMap<TString, TAttributeSchema*>& KeyToChild() const;

    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetAttribute(const TScalarAttributeSchema<TTypedObject, TTypedValue>& schema);
    template <class TOne, class TMany>
    TAttributeSchema* SetAttribute(const TManyToOneAttributeSchema<TMany, TOne>& schema);

    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetProtobufEvaluator(const TScalarAttributeSchema<TTypedObject, TString>& schema);
    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetProtobufSetter(const TScalarAttributeSchema<TTypedObject, TString>& schema);

    TAttributeSchema* SetAnnotationsAttribute();
    bool IsAnnotationsAttribute() const;

    TAttributeSchema* SetIdAttribute();
    TAttributeSchema* SetParentIdAttribute();

    TAttributeSchema* SetControlAttribute();

    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetControl(std::function<void(
        TTransaction*,
        TTypedObject*,
        const TTypedValue&)> control);

    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetValueSetter(std::function<void(
        TTransaction*,
        TTypedObject*,
        const NYT::NYPath::TYPath&,
        const TTypedValue&,
        bool)> setter);
    bool HasValueSetter() const;
    void RunValueSetter(
        TTransaction* transaction,
        TObject* object,
        const NYT::NYPath::TYPath& path,
        const NYT::NYTree::INodePtr& value,
        bool recursive);

    bool HasValueGetter() const;
    NYT::NYTree::INodePtr RunValueGetter(TObject* object) const;

    bool HasStoreScheduledGetter() const;
    bool RunStoreScheduledGetter(TObject* object) const;

    bool HasInitializer() const;
    void RunInitializer(
        TTransaction* transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetUpdatePrehandler(std::function<void(
        TTransaction*,
        TTypedObject*)> prehandler);
    void RunUpdatePrehandlers(
        TTransaction* transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetUpdateHandler(std::function<void(
        TTransaction*,
        TTypedObject*)> handler);
    void RunUpdateHandlers(
        TTransaction* transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetValidator(std::function<void(
        TTransaction*,
        TTypedObject*)> handler);
    void RunValidators(
        TTransaction* transaction,
        TObject* object);

    bool HasRemover() const;
    void RunRemover(
        TTransaction* transaction,
        TObject* object,
        const NYT::NYPath::TYPath& path);

    bool HasPreupdater() const;
    void RunPreupdater(
        TTransaction* transaction,
        TObject* object,
        const TUpdateRequest& request);

    TAttributeSchema* SetExpressionBuilder(std::function<NYT::NQueryClient::NAst::TExpressionPtr(
        IQueryContext*)> builder);
    bool HasExpressionBuilder() const;
    NYT::NQueryClient::NAst::TExpressionPtr RunExpressionBuilder(
        IQueryContext* context,
        const NYT::NYPath::TYPath& path);

    template <class TTypedObject>
    TAttributeSchema* SetPreevaluator(std::function<void(
        TTransaction*,
        TTypedObject*)> preevaluator);
    bool HasPreevaluator() const;
    void RunPreevaluator(
        TTransaction* transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetEvaluator(std::function<void(
        TTransaction*,
        TTypedObject*,
        NYson::IYsonConsumer*)> evaluator);
    bool HasEvaluator() const;
    void RunEvaluator(
        TTransaction* transaction,
        TObject* object,
        NYson::IYsonConsumer* consumer);

    bool HasTimestampPregetter() const;
    void RunTimestampPregetter(
        TTransaction* transaction,
        TObject* object,
        const NYT::NYPath::TYPath& path);

    bool HasTimestampGetter() const;
    TTimestamp RunTimestampGetter(
        TTransaction* transaction,
        TObject* object,
        const NYT::NYPath::TYPath& path);

    TAttributeSchema* EnableHistory(
        THistoryEnabledAttributeSchema schema = THistoryEnabledAttributeSchema());
    //! Returns all subattribute paths with enabled history.
    std::vector<NYPath::TYPath> GetHistoryEnabledAttributePaths() const;
    //! Returns all subattribute values with enabled history regardless of
    //! the filtering result and whether attribute was updated or not.
    //! Returns nullptr if there is no history enabled attribute.
    NYTree::INodePtr GetHistoryEnabledAttributes(TObject* object) const;
    //! Returns true iff there is at least one history enabled attribute for store,
    //! i.e. updated (store scheduled) and accepted by the history filter.
    bool HasHistoryEnabledAttributeForStore(TObject* object) const;

private:
    IObjectTypeHandler* const TypeHandler_;
    TObjectManager* const ObjectManager_;
    const TString Name_;

    THashMap<TString, TAttributeSchema*> KeyToChild_;
    TAttributeSchema* EtcChild_ = nullptr;
    TAttributeSchema* Parent_ = nullptr;

    std::function<void(TTransaction*, TObject*, const NYT::NYPath::TYPath&, const NYT::NYTree::INodePtr&, bool)> ValueSetter_;
    std::function<NYT::NYTree::INodePtr(TObject*)> ValueGetter_;
    std::function<bool(TObject*)> StoreScheduledGetter_;
    std::function<void(TTransaction*, TObject*)> Initializer_;
    std::vector<std::function<void(TTransaction*, TObject*)>> UpdatePrehandlers_;
    std::vector<std::function<void(TTransaction*, TObject*)>> UpdateHandlers_;
    std::vector<std::function<void(TTransaction*, TObject*)>> Validators_;
    std::function<void(TTransaction*, TObject*, const NYT::NYPath::TYPath&)> Remover_;
    std::function<void(TTransaction*, TObject*, const TUpdateRequest&)> Preupdater_;
    std::function<NYT::NQueryClient::NAst::TExpressionPtr(IQueryContext*, const NYT::NYPath::TYPath&)> ExpressionBuilder_;
    std::function<void(TTransaction*, TObject*)> Preevaluator_;
    std::function<void(TTransaction*, TObject*, NYson::IYsonConsumer*)> Evaluator_;
    std::function<void(TTransaction*, TObject*, const NYT::NYPath::TYPath&)> TimestampPregetter_;
    std::function<TTimestamp(TTransaction*, TObject*, const NYT::NYPath::TYPath&)> TimestampGetter_;

    // NB! For simplicity supports at most one history enabled subattribute.
    std::optional<THistoryEnabledAttributeSchema> HistoryEnabledAttribute_;

    bool Composite_ = false;
    bool Extensible_ = false;
    bool Mandatory_ = false;
    bool Updatable_ = false;
    bool Annotations_ = false;
    bool Opaque_ = false;
    bool Control_ = false;
    bool Etc_ = false;
    NAccessControl::EAccessControlPermission ReadPermission_ = NAccessControl::EAccessControlPermission::None;

    using TPathValidator = std::function<void(
        const TAttributeSchema*,
        const NYPath::TYPath&)>;
    void InitExpressionBuilder(const TDBField* field, TPathValidator pathValidtor);

    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitValueSetter(const TSchema& schema);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitValueGetter(const TSchema& schema);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitStoreScheduledGetter(const TSchema& schema);
    template <class TTypedObject, class TSchema>
    void InitTimestampGetter(const TSchema& schema);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitInitializer(const TSchema& schema);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitRemover(const TSchema& schema);
    template <class TTypedObject, class TSchema>
    void InitPreupdater(const TSchema& schema);

    void FillHistoryEnabledAttributePaths(
        std::vector<NYT::NYPath::TYPath>* result) const;
    NYTree::INodePtr GetHistoryEnabledAttributesImpl(
        TObject* object,
        bool hasHistoryEnabledParentAttribute) const;
    bool HasHistoryEnabledAttributeForStoreImpl(
        TObject* object,
        bool hasAcceptedByHistoryFilterParentAttribute) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

#define  ATTRIBUTE_SCHEMA_INL_H_
#include "attribute_schema-inl.h"
#undef ATTRIBUTE_SCHEMA_INL_H
