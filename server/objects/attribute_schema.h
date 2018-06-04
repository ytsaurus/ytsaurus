#pragma once

#include "object.h"
#include "transaction.h"

// TODO(babenko): replace with public
#include <yt/ytlib/query_client/ast.h>

#include <yt/core/yson/public.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IQueryContext
{
    virtual ~IQueryContext() = default;

    virtual NYT::NQueryClient::NAst::TExpressionPtr GetFieldExpression(const TDBField* field) = 0;
    virtual NYT::NQueryClient::NAst::TExpressionPtr GetAnnotationExpression(const TString& name) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TAttributeSchema
    : private TNonCopyable
{
public:
    TAttributeSchema(
        IObjectTypeHandler* typeHandler,
        const TString& name);

    bool IsOpaque() const;
    const TString& GetName() const;
    TString GetPath() const;

    TAttributeSchema* GetParent() const;
    void SetParent(TAttributeSchema* parent);

    TAttributeSchema* SetComposite();
    bool IsComposite() const;

    TAttributeSchema* SetMandatory();
    bool GetMandatory() const;

    TAttributeSchema* SetUpdatable();
    bool GetUpdatable() const;

    TAttributeSchema* SetFallback();
    bool IsFallback() const;

    void AddChild(TAttributeSchema* child);
    TAttributeSchema* AddChildren(const std::vector<TAttributeSchema*>& children);
    TAttributeSchema* FindChild(const TString& key) const;
    TAttributeSchema* FindFallbackChild() const;
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

    TAttributeSchema* SetParentAttribute();

    TAttributeSchema* SetControlAttribute();

    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetSetter(std::function<void(
        const TTransactionPtr&,
        TTypedObject*,
        const NYT::NYPath::TYPath&,
        const TTypedValue&,
        bool)> setter);
    template <class TTypedObject, class TTypedValue>
    TAttributeSchema* SetControl(std::function<void(
        const TTransactionPtr&,
        TTypedObject*,
        const TTypedValue&)> control);

    bool HasSetter() const;
    void RunSetter(
        const TTransactionPtr& transaction,
        TObject* object,
        const NYT::NYPath::TYPath& path,
        const NYT::NYTree::INodePtr& value,
        bool recursive);

    bool HasInitializer() const;
    void RunInitializer(
        const TTransactionPtr& transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetUpdateHandler(std::function<void(
        const TTransactionPtr&,
        TTypedObject*)> handler);
    void RunUpdateHandlers(
        const TTransactionPtr& transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetValidator(std::function<void(
        const TTransactionPtr&,
        TTypedObject*)> handler);
    void RunValidators(
        const TTransactionPtr& transaction,
        TObject* object);

    bool HasRemover() const;
    void RunRemover(
        const TTransactionPtr& transaction,
        TObject* object,
        const NYT::NYPath::TYPath& path);

    bool HasPreloader() const;
    void RunPreloader(
        const TTransactionPtr& transaction,
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
        const TTransactionPtr&,
        TTypedObject*)> preevaluator);
    bool HasPreevaluator() const;
    void RunPreevaluator(
        const TTransactionPtr& transaction,
        TObject* object);

    template <class TTypedObject>
    TAttributeSchema* SetEvaluator(std::function<void(
        const TTransactionPtr&,
        TTypedObject*,
        NYson::IYsonConsumer*)> evaluator);
    bool HasEvaluator() const;
    void RunEvaluator(
        const TTransactionPtr& transaction,
        TObject* object,
        NYson::IYsonConsumer* consumer);

private:
    IObjectTypeHandler* const TypeHandler_;
    const TString Name_;

    THashMap<TString, TAttributeSchema*> KeyToChild_;
    TAttributeSchema* FallbackChild_ = nullptr;
    TAttributeSchema* Parent_ = nullptr;

    std::function<void(const TTransactionPtr&, TObject*, const NYT::NYPath::TYPath&, const NYT::NYTree::INodePtr&, bool)> Setter_;
    std::function<void(const TTransactionPtr&, TObject*)> Initializer_;
    std::vector<std::function<void(const TTransactionPtr&, TObject*)>> UpdateHandlers_;
    std::vector<std::function<void(const TTransactionPtr&, TObject*)>> Validators_;
    std::function<void(const TTransactionPtr&, TObject*, const NYT::NYPath::TYPath&)> Remover_;
    std::function<void(const TTransactionPtr&, TObject*, const TUpdateRequest&)> Preloader_;
    std::function<NYT::NQueryClient::NAst::TExpressionPtr(IQueryContext*, const NYT::NYPath::TYPath&)> ExpressionBuilder_;
    std::function<void(const TTransactionPtr&, TObject*)> Preevaluator_;
    std::function<void(const TTransactionPtr&, TObject*, NYson::IYsonConsumer*)> Evaluator_;

    bool Composite_ = false;
    bool Mandatory_ = false;
    bool Updatable_ = false;
    bool Annotations_ = false;
    bool Opaque_ = false;
    bool Fallback_ = false;


    void InitExpressionBuilder(const TDBField* field, const char* udfFormatter = nullptr);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitSetter(const TSchema& schema);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitInitializer(const TSchema& schema);
    template <class TTypedObject, class TTypedValue, class TSchema>
    void InitRemover(const TSchema& schema);
    template <class TTypedObject, class TSchema>
    void InitPreloader(const TSchema& schema);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TScalarTypeTraits
{
    static const char* GetFormatterUdf()
    {
        return nullptr;
    }
};

template <>
struct TScalarTypeTraits<EPodCurrentState>
{
    static const char* GetFormatterUdf()
    {
        return "format_pod_current_state";
    }
};

template <>
struct TScalarTypeTraits<EPodTargetState>
{
    static const char* GetFormatterUdf()
    {
        return "format_pod_target_state";
    }
};

template <>
struct TScalarTypeTraits<EHfsmState>
{
    static const char* GetFormatterUdf()
    {
        return "format_hfsm_state";
    }
};

template <>
struct TScalarTypeTraits<EResourceKind>
{
    static const char* GetFormatterUdf()
    {
        return "format_resource_kind";
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

#define  ATTRIBUTE_SCHEMA_INL_H_
#include "attribute_schema-inl.h"
#undef ATTRIBUTE_SCHEMA_INL_H_