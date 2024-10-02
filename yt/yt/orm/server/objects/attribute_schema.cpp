#include "attribute_schema.h"

#include "helpers.h"
#include "object_manager.h"
#include "private.h"
#include "type_handler.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/library/attributes/ytree.h>

#include <yt/yt/orm/server/objects/object_reflection.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/ypath_filtering_consumer.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/join.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NAccessControl;

using namespace NAttributes;
using namespace NClient::NObjects;

using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NYPath;
using namespace NYT::NQueryClient::NAst;

using NYT::NYson::TYsonString;
using NYT::NYson::IYsonConsumer;
using NYT::NYPath::ETokenType;
using NYT::NYPath::TTokenizer;
using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const std::set<TYPath> DefaultHistoryPaths{""};

bool AreTrivialHistoryPaths(const std::set<TYPath>& paths)
{
    return paths == DefaultHistoryPaths;
}

struct TProtobufElementTypeResolver
{
    TProtobufElementTypeResolver(TYPathBuf path)
        : Path(path)
    { }

    TYPathBuf Path;

    EAttributeType operator()(const std::unique_ptr<TProtobufScalarElement>& element) const
    {
        if (!Path.empty()) {
            THROW_ERROR_EXCEPTION("Can not resolve path %v from scalar protobuf element", Path);
        }
        return ValueTypeToAttributeType(ProtobufToTableValueType(element->Type, element->EnumStorageType));
    }

    EAttributeType operator()(const std::unique_ptr<TProtobufMapElement>& element) const
    {
        if (Path.empty()) {
            return EAttributeType::Map;
        }
        NYPath::TTokenizer tokenizer(Path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        return ResolveAttributeTypeByProtobufElement(element->Element, tokenizer.GetSuffix());
    }

    EAttributeType operator()(const std::unique_ptr<TProtobufRepeatedElement>& element) const
    {
        if (Path.empty()) {
            return EAttributeType::List;
        }
        NYPath::TTokenizer tokenizer(Path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.ExpectListIndex();
        return ResolveAttributeTypeByProtobufElement(element->Element, tokenizer.GetSuffix());
    }

    EAttributeType operator()(const std::unique_ptr<TProtobufMessageElement>& element) const
    {
        if (Path.empty()) {
            return EAttributeType::Message;
        }
        auto result = ResolveProtobufElementByYPath(element->Type, Path);
        return ResolveAttributeTypeByProtobufElement(result.Element, result.TailPath);
    }

    EAttributeType operator()(const std::unique_ptr<TProtobufAttributeDictionaryElement>& element) const
    {
        if (Path.empty()) {
            return EAttributeType::AttributeDictionary;
        }
        auto result = ResolveProtobufElementByYPath(element->Type, Path);
        return ResolveAttributeTypeByProtobufElement(result.Element, result.TailPath);
    }

    EAttributeType operator()(const std::unique_ptr<TProtobufAnyElement>&) const
    {
        return EAttributeType::Any;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void DoEvaluateViewAttribute(
    const TAttributeSchema* viewAttributeSchema,
    TTransaction* transaction,
    const TObject* object,
    NYson::IYsonConsumer* consumer)
{
    if (auto* compositeViewSchema = viewAttributeSchema->TryAsComposite()) {
        consumer->OnBeginMap();
        compositeViewSchema->ForEachImmediateChild(
            [transaction, object, consumer] (const TAttributeSchema* schema)
        {
            if (auto* scalarSchema = schema->TryAsScalar(); scalarSchema && scalarSchema->IsEtc()) {
                TUnwrappingConsumer unwrappingConsumer(consumer);
                DoEvaluateViewAttribute(schema, transaction, object, &unwrappingConsumer);
                return;
            }

            if (schema->IsControl() || schema->IsOpaque() && !schema->EmitOpaqueAsEntity()) {
                return;
            }
            consumer->OnKeyedItem(schema->GetName());
            if (schema->IsOpaque()) {
                consumer->OnEntity();
            } else {
                DoEvaluateViewAttribute(schema, transaction, object, consumer);
            }
        });
        consumer->OnEndMap();
    } else {
        auto* scalarViewSchema = viewAttributeSchema->TryAsScalar();

        if (scalarViewSchema->HasValueGetter()) {
            scalarViewSchema->RunValueGetter(transaction, object, consumer, NYPath::TYPath{});
        }
    }
}

void EvaluateViewAttribute(
    const TAttributeSchema* viewAttributeSchema,
    TTransaction* transaction,
    const TObject* object,
    NYson::IYsonConsumer* consumer,
    const NYPath::TYPath& path)
{
    TValuePresentConsumer valuePresentConsumer(consumer);
    auto pathConsumer = NYson::CreateYPathDesignatedConsumer(
        path,
        NYson::EMissingPathMode::Ignore,
        &valuePresentConsumer);
    DoEvaluateViewAttribute(viewAttributeSchema, transaction, object, pathConsumer.get());
    if (!valuePresentConsumer.IsValuePresent()) {
        consumer->OnEntity();
    }
}

void ValidateString(std::string_view value, EUtf8Check check, TYPathBuf path)
{
    if (check == EUtf8Check::Disable || IsUtf(value)) {
        return;
    }
    switch (check) {
        case EUtf8Check::Disable:
            return;
        case EUtf8Check::LogOnFail:
            YT_LOG_WARNING("String field got non UTF-8 value (Path: %v, Value: %v)", path, value);
            return;
        case EUtf8Check::ThrowOnFail:
            THROW_ERROR_EXCEPTION("Non UTF-8 value in string field %v", path)
                << TErrorAttribute("non_utf8_string", value);
    }
}

void ValidateString(const std::vector<TString>& values, EUtf8Check check, TYPathBuf path)
{
    if (check == EUtf8Check::Disable) {
        return;
    }
    for (const auto& value : values) {
        NDetail::ValidateString(value, check, path);
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TAttributeSchema* TAttributeSchema::SetControlAttribute()
{
    Control_ = true;
    return this;
}

TAttributeSchema::TAttributeSchema(
    IObjectTypeHandler* typeHandler,
    TObjectManager* objectManager,
    std::string name)
    : TypeHandler_(typeHandler)
    , ObjectManager_(objectManager)
    , Name_(std::move(name))
{ }

TAttributeSchema* TAttributeSchema::SetComputed()
{
    Computed_ = true;
    return this;
}

bool TAttributeSchema::IsComputed() const
{
    return Computed_;
}

TAttributeSchema* TAttributeSchema::SetOpaque(bool emitEntity)
{
    YT_VERIFY(!TryAsScalar() || !TryAsScalar()->IsEtc());
    Opaque_ = true;
    EmitOpaqueAsEntity_ = emitEntity;
    return this;
}

bool TAttributeSchema::IsOpaque() const
{
    return Opaque_;
}

bool TAttributeSchema::EmitOpaqueAsEntity() const
{
    return EmitOpaqueAsEntity_;
}

TAttributeSchema* TAttributeSchema::SetOpaqueForHistory()
{
    YT_VERIFY(!HistoryEnabledAttribute_);
    OpaqueForHistory_ = true;
    return this;
}

bool TAttributeSchema::IsOpaqueForHistory() const
{
    return OpaqueForHistory_;
}

TAttributeSchema* TAttributeSchema::SetUpdatePolicy(EUpdatePolicy policy, TYPath path)
{
    UpdatePolicy_ = policy;
    bool superuserOnly = policy == EUpdatePolicy::ReadOnly || policy == EUpdatePolicy::OpaqueReadOnly;

    // Related updatable and read only updatable attributes contradict each other,
    // so such configurations are forbidden, e.g.: /spec and /spec/a.
    auto validateNoRelatedAttributes = [&] (const std::vector<TYPath>& otherPaths) {
        for (const auto& otherPath : otherPaths) {
            if (AreAttributesRelated(path, otherPath)) {
                THROW_ERROR_EXCEPTION(
                    "Updatable %Qv and superuser only updatable attribute %Qv contradict each other within %v",
                    superuserOnly ? otherPath : path,
                    superuserOnly ? path : otherPath,
                    FormatTypePath());
            }
        }
    };

    ValidateAttributePath(path);
    validateNoRelatedAttributes(superuserOnly ? UpdatablePaths_ : SuperuserOnlyUpdatablePaths_);

    if (superuserOnly) {
        SuperuserOnlyUpdatablePaths_.push_back(std::move(path));
    } else {
        UpdatablePaths_.push_back(std::move(path));
    }

    if (TryAsComposite() && !path.empty()) {
        THROW_ERROR_EXCEPTION(
            "Expected non empty path for composite attribute, got: %Qv",
            path);
    }
    if (IsOpaqueForUpdates() && !path.empty()) {
        THROW_ERROR_EXCEPTION(
            "Expected non empty path for opaque update policies, got: %Qv",
            path);
    }

    return this;
}

std::optional<EUpdatePolicy> TAttributeSchema::GetUpdatePolicy() const
{
    return UpdatePolicy_;
}

bool TAttributeSchema::IsOpaqueForUpdates() const
{
    if (!UpdatePolicy_) {
        return false;
    }
    return UpdatePolicy_.value() == EUpdatePolicy::OpaqueUpdatable ||
        UpdatePolicy_.value() == EUpdatePolicy::OpaqueReadOnly;
}

bool TAttributeSchema::IsControl() const
{
    return Control_;
}

bool TAttributeSchema::IsAnnotationsAttribute() const
{
    return Annotations_;
}

const std::string& TAttributeSchema::GetName() const
{
    return Name_;
}

TYPath TAttributeSchema::GetPath() const
{
    TCompactVector<const TAttributeSchema*, 4> parents;
    const auto* current = this;
    while (current->GetParent()) {
        if (current->TryAsComposite() || !current->TryAsScalar()->IsEtc()) {
            parents.push_back(current);
        }
        current = current->GetParent();
    }
    TStringBuilder builder;
    for (auto it = parents.rbegin(); it != parents.rend(); ++it) {
        builder.AppendChar('/');
        builder.AppendString(ToYPathLiteral((*it)->GetName()));
    }
    return builder.Flush();
}

std::string TAttributeSchema::FormatPathEtc() const
{
    if (TryAsScalar() && TryAsScalar()->IsEtc()) {
        const auto& name = GetName();
        if (name.empty()) {
            return Format("%v[etc]", GetPath());
        }
        return Format("%v[etc]@%v", GetPath(), name);
    } else {
        return GetPath();
    }
}

std::string TAttributeSchema::FormatTypePath() const
{
    return Format("%v attribute %v",
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TypeHandler_->GetType()),
        GetPath());
}

const TCompositeAttributeSchema* TAttributeSchema::GetParent() const
{
    return Parent_.get();
}

TCompositeAttributeSchema* TAttributeSchema::GetParent()
{
    return Parent_.get();
}

void TAttributeSchema::SetParent(TAttributeSchema* schema, TCompositeAttributeSchema* parent)
{
    YT_VERIFY(!schema->Parent_);
    schema->Parent_ = parent;
}

TAttributeSchema* TAttributeSchema::SetExtensible()
{
    Extensible_ = true;
    return this;
}

bool TAttributeSchema::IsExtensible(TYPathBuf path) const
{
    switch (ObjectManager_->GetAttributesExtensibilityMode()) {
        case EAttributesExtensibilityMode::None:
            return false;
        case EAttributesExtensibilityMode::Full:
            return HasExtensibleParent();
        case EAttributesExtensibilityMode::Restricted: {
            if (!HasExtensibleParent()) {
                return false;
            }
            auto fullPath = GetPath() + path;
            for (const auto& patternPath : ObjectManager_
                ->GetAllowedExtensibleAttributePaths(TypeHandler_->GetType()))
            {
                switch (MatchAttributePathToPattern(patternPath, fullPath)) {
                    case EAttributePathMatchResult::PatternIsPrefix:
                    case EAttributePathMatchResult::PathIsPrefix:
                    case EAttributePathMatchResult::Full:
                        return true;
                    case EAttributePathMatchResult::None:
                        break;
                }
            }
            return false;
        }
    }
}

TProtobufWriterOptions::TUnknownYsonFieldModeResolver
    TAttributeSchema::BuildUnknownYsonFieldModeResolver() const
{
    switch (ObjectManager_->GetAttributesExtensibilityMode()) {
        case EAttributesExtensibilityMode::None: {
            return TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
                EUnknownYsonFieldsMode::Fail);
        }
        case EAttributesExtensibilityMode::Full: {
            return HasExtensibleParent()
                ? TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
                    EUnknownYsonFieldsMode::Keep)
                : TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
                    EUnknownYsonFieldsMode::Fail);
        }
        case EAttributesExtensibilityMode::Restricted: {
            if (!HasExtensibleParent()) {
                return TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
                    EUnknownYsonFieldsMode::Fail);
            }
            return
                [basePath = GetPath(), patternPaths = ObjectManager_
                    ->GetAllowedExtensibleAttributePaths(TypeHandler_->GetType())]
                    (const TYPath& path) -> EUnknownYsonFieldsMode
                {
                    auto fullPath = basePath + path;
                    bool hasPatternAsPrefix = false;
                    bool isPrefixOfPattern = false;
                    for (const auto& patternPath : patternPaths) {
                        switch (MatchAttributePathToPattern(patternPath, fullPath)) {
                            case EAttributePathMatchResult::PatternIsPrefix:
                            case EAttributePathMatchResult::Full: {
                                hasPatternAsPrefix = true;
                                break;
                            }
                            case EAttributePathMatchResult::PathIsPrefix: {
                                isPrefixOfPattern = true;
                                break;
                            }
                            case EAttributePathMatchResult::None: {
                                break;
                            }
                            default: {
                                YT_ABORT();
                            }
                        }
                    }
                    if (hasPatternAsPrefix) {
                        return EUnknownYsonFieldsMode::Keep;
                    }
                    if (isPrefixOfPattern) {
                        return EUnknownYsonFieldsMode::Forward;
                    }
                    return EUnknownYsonFieldsMode::Fail;
                };
        }
        default: {
            YT_ABORT();
        }
    }
}

void TAttributeSchema::RunUpdatePrehandlers(TTransaction* transaction, const TObject* object) const
{
    for (const auto& prehandler : UpdatePrehandlers_) {
        prehandler(transaction, object);
    }
}

void TAttributeSchema::RunUpdateHandlerImpl(
    TTransaction* transaction,
    TObject* object,
    size_t handlerIndex,
    IUpdateContext* context) const
{
    YT_VERIFY(handlerIndex < UpdateHandlers_.size());
    UpdateHandlers_[handlerIndex](transaction, object);
    if (handlerIndex + 1 < UpdateHandlers_.size()) {
        context->AddFinalizer(std::bind_front(
            &TAttributeSchema::RunUpdateHandlerImpl, this, transaction, object, handlerIndex + 1));
    }
}

void TAttributeSchema::RunUpdateHandlers(
    TTransaction* transaction,
    TObject* object,
    IUpdateContext* context) const
{
    if (!UpdateHandlers_.empty()) {
        context->AddFinalizer(std::bind_front(
            &TAttributeSchema::RunUpdateHandlerImpl, this, transaction, object, 0));
    }
}

void TAttributeSchema::RunValidators(
    TTransaction* transaction,
    const TObject* object) const
{
    if (object->IsRemoved()) {
        return;
    }
    try {
        for (const auto& validator : Validators_) {
            validator(transaction, object);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating %Qv for %v",
            FormatPathEtc(),
            object->GetDisplayName())
            << ex;
    }
}

bool TAttributeSchema::HasPreloader() const
{
    return Preloader_.operator bool();
}

void TAttributeSchema::RunPreloader(const TObject* object, const NYPath::TYPath& path) const
{
    Preloader_(object, path);
}

// TODO(grigminakov): Validate composite only call after restricting ResolveAttribute
TExpressionPtr TAttributeSchema::RunListContainsExpressionBuilder(
    IQueryContext* context,
    TLiteralExpression* literalExpr,
    const TYPath& path,
    EAttributeExpressionContext expressionContext) const
{
    if (ListContainsExpressionBuilder_.operator bool()) {
        return ListContainsExpressionBuilder_(context, literalExpr, path, expressionContext);
    }
    return nullptr;
}

void TAttributeSchema::RunPostInitializers()
{
    for (const auto& postInitializer: PostInitializers_) {
        postInitializer();
    }
}

TAttributeSchema* TAttributeSchema::SetMandatory(const TYPath& path)
{
    InsertOrCrash(MandatoryPaths_, path);
    return this;
}

const THashSet<TYPath>& TAttributeSchema::GetMandatory() const
{
    return MandatoryPaths_;
}

TAttributeSchema* TAttributeSchema::SetUpdatable(TYPath path)
{
    SetUpdatePolicy(EUpdatePolicy::Updatable, path);
    return this;
}

bool TAttributeSchema::IsUpdatable(const TYPath& path) const
{
    for (const auto& updatablePath : UpdatablePaths_) {
        if (HasPrefix(path, updatablePath)) {
            return true;
        }
    }
    return false;
}

bool TAttributeSchema::IsSuperuserOnlyUpdatable(const TYPath& path) const
{
    for (const auto& updatablePath : SuperuserOnlyUpdatablePaths_) {
        if (HasPrefix(path, updatablePath)) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void THistoryEnabledAttributeSchema::Update(THistoryEnabledAttributeSchema schema)
{
    if (AreTrivialHistoryPaths(schema.Paths_)) {
        Paths_ = DefaultHistoryPaths;
    } else if (!AreTrivialHistoryPaths(Paths_)) {
        Paths_.merge(std::move(schema.Paths_));
    }
    if (schema.ValueFilter_) {
        ValueFilter_ = std::move(schema.ValueFilter_);
    }
    if (schema.ValueMutator_) {
        ValueMutator_ = std::move(schema.ValueMutator_);
    }
    Owner_ = schema.Owner_;
}

THistoryEnabledAttributeSchema& THistoryEnabledAttributeSchema::AddPath(TYPath path)
{
    if (!AreTrivialHistoryPaths(Paths_)) {
        Paths_.insert(std::move(path));
    }
    return *this;
}

const std::set<TYPath>& THistoryEnabledAttributeSchema::GetPaths() const
{
    return Paths_.empty()
        ? DefaultHistoryPaths
        : Paths_;
}

bool THistoryEnabledAttributeSchema::HasValueFilter() const
{
    return static_cast<bool>(ValueFilter_);
}

bool THistoryEnabledAttributeSchema::RunValueFilter(const TObject* object) const
{
    return !HasValueFilter() || ValueFilter_(object);
}

THistoryEnabledAttributeSchema& THistoryEnabledAttributeSchema::SetValueMutator(
    std::function<std::unique_ptr<NYson::IYsonConsumer>(NYson::IYsonConsumer*)> valueMutator)
{
    ValueMutator_ = std::move(valueMutator);
    return *this;
}

bool THistoryEnabledAttributeSchema::HasValueMutator() const
{
    return static_cast<bool>(ValueMutator_);
}

std::unique_ptr<NYson::IYsonConsumer> THistoryEnabledAttributeSchema::RunValueMutator(IYsonConsumer* consumer) const
{
    YT_VERIFY(ValueMutator_);
    return ValueMutator_(consumer);
}

THistoryEnabledAttributeSchema& THistoryEnabledAttributeSchema::SetOwner(const TAttributeSchema* owner)
{
    YT_VERIFY(!Owner_);
    Owner_ = owner;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TAttributeSchema* TAttributeSchema::EnableHistory(
    THistoryEnabledAttributeSchema schema)
{
    YT_VERIFY(!IsOpaqueForHistory());

    for (const auto& path : schema.GetPaths()) {
        ValidateAttributePath(path);
    }
    if (HistoryEnabledAttribute_) {
        HistoryEnabledAttribute_->Update(std::move(schema));
    } else {
        HistoryEnabledAttribute_ = std::move(schema);
    }
    const auto& paths = HistoryEnabledAttribute_->GetPaths();
    for (auto firstIt = paths.begin(); firstIt != paths.end(); ++firstIt) {
        auto secondIt = firstIt;
        ++secondIt;
        for (; secondIt != paths.end(); ++secondIt) {
            YT_VERIFY(!HasPrefix(*secondIt, *firstIt));
        }
    }

    HistoryEnabledAttribute_->SetOwner(this);
    if (HistoryEnabledAttribute_->HasValueFilter()) {
        ForEachAttribute([] (const TAttributeSchema* schema) {
            if (schema->ValueFilteredChildrenValidator_ &&
                !schema->ValueFilteredChildrenValidator_())
            {
                // All children of value filtered attributes must be configured in advance.
                YT_ABORT();
            }
            return false;
        });
    }

    return this;
}

std::set<TYPath> TAttributeSchema::GetHistoryEnabledAttributePaths() const
{
    std::set<TYPath> result;
    FillHistoryEnabledAttributePaths(&result);
    return result;
}

void TAttributeSchema::GetHistoryEnabledAttributes(TTransaction* transaction, const TObject* object, NAttributes::IYsonBuilder* builder) const
{
    GetHistoryEnabledAttributesImpl(transaction, object, builder, /*hasHistoryEnabledParent*/ false);
}

bool TAttributeSchema::HasHistoryEnabledAttributeForStore(const TObject* object, bool verboseLogging) const
{
    return ForEachAttributeWithHistoryEnabledParentImpl([object, verboseLogging] (
        const TAttributeSchema* schema,
        bool hasHistoryEnabledParentAttribute) -> std::optional<bool>
    {
        if (schema->IsAttributeAcceptableForHistory(hasHistoryEnabledParentAttribute) &&
            schema->ShouldStoreHistoryEnabledAttribute(object, verboseLogging))
        {
            return true;
        }

        // Attribute was rejected by the filter, skip checking subtree.
        if (schema->HistoryEnabledAttribute_ && schema->HistoryEnabledAttribute_->HasValueFilter()) {
            return false;
        }

        return std::nullopt;
    });
}

bool TAttributeSchema::HasStoreScheduledHistoryAttributes(const TObject* object) const
{
    return ForEachAttributeWithHistoryEnabledParentImpl([object] (
        const TAttributeSchema* schema,
        bool hasHistoryEnabledParentAttribute) -> std::optional<bool>
    {
        if (schema->IsAttributeAcceptableForHistory(hasHistoryEnabledParentAttribute)) {
            if (schema->HistoryEnabledAttribute_ && schema->HistoryEnabledAttribute_->HasValueFilter()) {
                return schema->HistoryEnabledAttribute_->RunValueFilter(object);
            } else {
                auto* scalarSchema = schema->TryAsScalar();
                if (!scalarSchema) {
                    return std::nullopt;
                }

                if (scalarSchema->HasChangedGetter() && scalarSchema->GetChangedGetterType() != EChangedGetterType::Default) {
                    return true;
                } else if (scalarSchema->HasStoreScheduledGetter()) {
                    if (scalarSchema->RunStoreScheduledGetter(object)) {
                        return true;
                    } else {
                        return std::nullopt;
                    }
                } else {
                    return true;
                }
            }
        }

        return std::nullopt;
    });
}

void TAttributeSchema::PreloadHistoryEnabledAttributes(const TObject* object) const
{
    ForEachAttributeWithHistoryEnabledParentImpl([object] (
        const TAttributeSchema* schema,
        bool hasHistoryEnabledParentAttribute) -> std::optional<bool>
    {
        if (schema->IsAttributeAcceptableForHistory(hasHistoryEnabledParentAttribute))
        {
            auto hasNonConstantChangedGetter = false;
            auto hasValueGetter = false;
            if (auto* scalarSchema = schema->TryAsScalar()) {
                hasNonConstantChangedGetter = scalarSchema->HasChangedGetter() &&
                    scalarSchema->GetChangedGetterType() != EChangedGetterType::ConstantTrue &&
                    scalarSchema->GetChangedGetterType() != EChangedGetterType::ConstantFalse;
                hasValueGetter = scalarSchema->HasValueGetter();
            }

            if (schema->HasPreloader() && (hasValueGetter || hasNonConstantChangedGetter)) {
                schema->RunPreloader(object);
            }
        }

        return std::nullopt;
    });
}

bool TAttributeSchema::IsHistoryEnabledFor(const TYPath& suffix) const
{
    if (!HistoryEnabledAttribute_) {
        return Parent_ ? Parent_->IsHistoryEnabledFor("/" + GetName()) : false;
    }
    const auto& paths = HistoryEnabledAttribute_->GetPaths();
    for (const auto& path : paths) {
        if (HasPrefix(suffix, path)) {
            return true;
        }
    }
    return false;
}

bool TAttributeSchema::ShouldStoreHistoryIndexedAttribute(const TObject* object, const TYPath& suffix) const
{
    return ShouldStoreHistoryAttributeImpl(object, {suffix}, /*fromIndex*/ true, /*verboseLogging*/ false);
}

////////////////////////////////////////////////////////////////////////////////

void TAttributeSchema::FillHistoryEnabledAttributePaths(std::set<TYPath>* result) const
{
    if (HistoryEnabledAttribute_) {
        for (const auto& path : HistoryEnabledAttribute_->GetPaths()) {
            result->insert(GetPath() + path);
        }
    }

    if (const auto* compositeSchema = TryAsComposite()) {
        for (const auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
            etcChild->FillHistoryEnabledAttributePaths(result);
        }

        for (const auto& [key, child] : compositeSchema->KeyToChild()) {
            child->FillHistoryEnabledAttributePaths(result);
        }
    }
}

// TODO(grigminakov): Get rid of parallel stack and simplify this function in YTORM-638.
bool TAttributeSchema::GetHistoryEnabledAttributesImpl(
    TTransaction* transaction,
    const TObject* object,
    NAttributes::IYsonBuilder* builder,
    bool hasHistoryEnabledParentAttribute) const
{
    bool hasHistoryEnabledAttribute = hasHistoryEnabledParentAttribute || HistoryEnabledAttribute_;

    std::unique_ptr<NYson::IYsonConsumer> valueMutatorHolder;
    std::unique_ptr<NAttributes::IYsonBuilder> builderHolder;
    if (HistoryEnabledAttribute_ && HistoryEnabledAttribute_->HasValueMutator()) {
        valueMutatorHolder = HistoryEnabledAttribute_->RunValueMutator(builder->GetConsumer());
        builderHolder = std::make_unique<NAttributes::TYsonBuilder>(EYsonBuilderForwardingPolicy::Crash, builder, valueMutatorHolder.get());
        builder = builderHolder.get();
    }

    if (auto* compositeSchema = TryAsComposite()) {
        bool hasChilds = false;

        builder->GetConsumer()->OnBeginMap();
        YT_VERIFY(!HistoryEnabledAttribute_ || AreTrivialHistoryPaths(HistoryEnabledAttribute_->GetPaths()));
        for (const auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
            TUnwrappingConsumer unwrappingConsumer(builder->GetConsumer());
            NAttributes::TYsonBuilder newBuilder(EYsonBuilderForwardingPolicy::Forward, builder, &unwrappingConsumer);
            if (etcChild->GetHistoryEnabledAttributesImpl(transaction, object, &newBuilder, hasHistoryEnabledAttribute)) {
                hasChilds = true;
            }
        }

        for (const auto& [key, child] : compositeSchema->KeyToChild()) {
            auto checkpoint = builder->CreateCheckpoint();
            builder->GetConsumer()->OnKeyedItem(key);
            if (child->GetHistoryEnabledAttributesImpl(transaction, object, builder, hasHistoryEnabledAttribute)) {
                hasChilds = true;
            } else {
                builder->RestoreCheckpoint(checkpoint);
            }
        }
        builder->GetConsumer()->OnEndMap();
        return hasChilds;
    }

    const auto* scalarAttribute = this->TryAsScalar();

    if (!IsAttributeAcceptableForHistory(hasHistoryEnabledParentAttribute)) {
        return false;
    }
    if (!scalarAttribute->HasValueGetter()) {
        return false;
    }

    if (hasHistoryEnabledParentAttribute ||
        AreTrivialHistoryPaths(HistoryEnabledAttribute_->GetPaths()))
    {
        scalarAttribute->RunValueGetter(transaction, object, builder->GetConsumer());
        return true;
    }

    const auto& paths = HistoryEnabledAttribute_->GetPaths();
    auto pathFilteredConsumer = CreateYPathFilteringConsumer(
        builder->GetConsumer(),
        {paths.begin(), paths.end()},
        EYPathFilteringMode::WhitelistWithForcedEntities);
    scalarAttribute->RunValueGetter(transaction, object, pathFilteredConsumer.get());
    return true;
}

bool TAttributeSchema::ShouldStoreHistoryAttributeImpl(
    const TObject* object,
    const std::set<TYPath>& paths,
    bool fromIndex,
    bool verboseLogging) const
{
    bool isAccepted = false;
    std::string method = "";
    auto* scalarSchema = TryAsScalar();
    if (HistoryEnabledAttribute_ && HistoryEnabledAttribute_->HasValueFilter()) {
        isAccepted = HistoryEnabledAttribute_->RunValueFilter(object);
        method = "HistoryFilter";
    } else if (scalarSchema && scalarSchema->HasChangedGetter()) {
        isAccepted = std::any_of(
            paths.begin(),
            paths.end(),
            [&] (const auto& path)
        {
            return scalarSchema->RunChangedGetter(object, path);
        });
        method = "ChangedGetter";
    } else if (scalarSchema && scalarSchema->HasStoreScheduledGetter()) {
        isAccepted = scalarSchema->RunStoreScheduledGetter(object);
        method = "StoreScheduledGetter";
    } else if (TryAsComposite()) {
        YT_VERIFY(AreTrivialHistoryPaths(paths));
        return false;
    } else {
        // There must be no attributes with enabled history and undefined method of getting their current status.
        YT_ABORT();
    }
    YT_LOG_DEBUG_IF(verboseLogging,
        "Checking if attribute is accepted for history store "
        "(ObjectKey: %v, AttributePath: %v, Accepted: %v, Method: %v%v, FromIndex: %v)",
        object->GetKey(),
        FormatPathEtc(),
        isAccepted,
        method,
        AreTrivialHistoryPaths(paths) ? "" : Format(", Subpaths: %v", paths),
        fromIndex);
    return isAccepted;
}

bool TAttributeSchema::ShouldStoreHistoryEnabledAttribute(const TObject* object, bool verboseLogging) const
{
    const auto& paths = HistoryEnabledAttribute_
        ? HistoryEnabledAttribute_->GetPaths()
        : DefaultHistoryPaths;
    return ShouldStoreHistoryAttributeImpl(object, paths, /*fromIndex*/ false, verboseLogging);
}

bool TAttributeSchema::IsAttributeAcceptableForHistory(bool hasAcceptedByHistoryParentAttribute) const
{
    bool isAcceptableAsScalar = false;
    if (const auto* scalarAttribute = TryAsScalar(); scalarAttribute) {
        isAcceptableAsScalar = scalarAttribute->HasChangedGetter() ||
            scalarAttribute->HasValueGetter() ||
            scalarAttribute->HasStoreScheduledGetter();
    }
    return HistoryEnabledAttribute_ ||
        (hasAcceptedByHistoryParentAttribute && !IsOpaque() && !IsOpaqueForHistory() &&
            (isAcceptableAsScalar || TryAsComposite()));
}

////////////////////////////////////////////////////////////////////////////////

TAttributeSchema* TAttributeSchema::EnableMetaResponseAttribute(bool enable)
{
    MetaResponseAttribute_ = enable;
    return this;
}

void TAttributeSchema::PreloadMetaResponseAttributes(const TObject* object) const
{
    ForEachLeafAttribute([object] (const TAttributeSchema* schema) {
        if (schema->MetaResponseAttribute_ && schema->HasPreloader()) {
            schema->RunPreloader(object);
        }
        return false;
    });
}

bool TAttributeSchema::IsMetaResponseAttribute() const
{
    return MetaResponseAttribute_;
}

////////////////////////////////////////////////////////////////////////////////

bool TAttributeSchema::ForEachAttributeImpl(const TOnAttribute& onAttribute, bool leafOnly) const
{
    if (auto* compositeSchema = TryAsComposite()) {
        if (!leafOnly && onAttribute(this)) {
            return true;
        }

        for (const auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
            if (etcChild->ForEachAttributeImpl(onAttribute, leafOnly)) {
                return true;
            }
        }

        for (const auto& [key, child] : compositeSchema->KeyToChild()) {
            if (child->ForEachAttributeImpl(onAttribute, leafOnly)) {
                return true;
            }
        }
        return false;
    } else {
        return onAttribute(this);
    }
}

void TAttributeSchema::ForEachLeafAttribute(const TOnScalarAttribute& onAttribute) const
{
    ForEachAttributeImpl(
        [&onAttribute] (const TAttributeSchema* schema) {
            auto scalarSchema = schema->TryAsScalar();
            YT_VERIFY(scalarSchema);
            return onAttribute(scalarSchema);
        },
        true);
}

void TAttributeSchema::ForEachAttribute(const TOnAttribute& onAttribute) const
{
    ForEachAttributeImpl(onAttribute, false);
}

bool TAttributeSchema::ForEachAttributeMutableImpl(const TOnMutableAttribute& onAttribute, bool leafOnly)
{
    if (auto* compositeSchema = TryAsComposite()) {
        if (!leafOnly && onAttribute(this)) {
            return true;
        }

        for (auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
            if (etcChild->ForEachAttributeMutableImpl(onAttribute, leafOnly)) {
                return true;
            }
        }

        for (auto& [key, child] : compositeSchema->KeyToChild()) {
            if (child->ForEachAttributeMutableImpl(onAttribute, leafOnly)) {
                return true;
            }
        }
        return false;
    } else {
        return onAttribute(this);
    }
}

void TAttributeSchema::ForEachLeafAttributeMutable(const TOnMutableAttribute& onAttribute)
{
    ForEachAttributeMutableImpl(onAttribute, true);
}

void TAttributeSchema::ForEachAttributeMutable(const TOnMutableAttribute& onAttribute)
{
    ForEachAttributeMutableImpl(onAttribute, false);
}

bool TAttributeSchema::ForEachAttributeWithHistoryEnabledParentImpl(
    const TOnAttributeWithHistoryEnabledParent& onAttribute,
    bool hasHistoryEnabledParentAttribute) const
{
    hasHistoryEnabledParentAttribute |= HistoryEnabledAttribute_.has_value();
    if (auto forwardedValue = onAttribute(this, hasHistoryEnabledParentAttribute)) {
        return *forwardedValue;
    }

    if (const auto* compositeSchema = TryAsComposite()) {
        for (const auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
            if (etcChild->ForEachAttributeWithHistoryEnabledParentImpl(onAttribute, hasHistoryEnabledParentAttribute)) {
                return true;
            }
        }

        for (const auto& [key, child] : compositeSchema->KeyToChild()) {
            if (child->ForEachAttributeWithHistoryEnabledParentImpl(onAttribute, hasHistoryEnabledParentAttribute)) {
                return true;
            }
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandler* TAttributeSchema::GetTypeHandler() const
{
    return TypeHandler_;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeAttributeSchema* TAttributeSchema::TryAsComposite()
{
    return dynamic_cast<TCompositeAttributeSchema*>(this);
}

const TCompositeAttributeSchema* TAttributeSchema::TryAsComposite() const
{
    return dynamic_cast<const TCompositeAttributeSchema*>(this);
}

TCompositeAttributeSchema* TAttributeSchema::AsComposite()
{
    auto* schema = TryAsComposite();
    THROW_ERROR_EXCEPTION_UNLESS(schema, "Attribute %v is not composite", FormatPathEtc());
    return schema;
}

const TCompositeAttributeSchema* TAttributeSchema::AsComposite() const
{
    const auto* schema = TryAsComposite();
    THROW_ERROR_EXCEPTION_UNLESS(schema, "Attribute %v is not composite", FormatPathEtc());
    return schema;
}

TScalarAttributeSchema* TAttributeSchema::TryAsScalar()
{
    return dynamic_cast<TScalarAttributeSchema*>(this);
}

const TScalarAttributeSchema* TAttributeSchema::TryAsScalar() const
{
    return dynamic_cast<const TScalarAttributeSchema*>(this);
}

TScalarAttributeSchema* TAttributeSchema::AsScalar()
{
    auto* schema = TryAsScalar();
    THROW_ERROR_EXCEPTION_UNLESS(schema, "Attribute %v is not scalar", FormatPathEtc());
    return schema;
}

const TScalarAttributeSchema* TAttributeSchema::AsScalar() const
{
    const auto* schema = TryAsScalar();
    THROW_ERROR_EXCEPTION_UNLESS(schema, "Attribute %v is not scalar", FormatPathEtc());
    return schema;
}

////////////////////////////////////////////////////////////////////////////////

TAttributeSchema* TAttributeSchema::SetReadPermission(TAccessControlPermissionValue permission)
{
    if (permission != TAccessControlPermissionValues::None) {
        Opaque_ = true;
    }
    ReadPermission_ = std::move(permission);
    return this;
}

TAccessControlPermissionValue TAttributeSchema::GetReadPermission() const
{
    return ReadPermission_;
}

bool TAttributeSchema::HasExtensibleParent() const
{
    const auto* current = this;
    while (current) {
        if (current->Extensible_) {
            return true;
        }
        current = current->GetParent();
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

NClient::NObjects::TObjectTypeValue TAttributeSchema::GetViewObjectType() const
{
    return ViewObjectType_;
}

void TAttributeSchema::SetView()
{
    View_ = true;
}

bool TAttributeSchema::IsView() const
{
    return View_;
}

////////////////////////////////////////////////////////////////////////////////

void TScalarAttributeSchema::SetProtobufElement(NYson::TProtobufElement element)
{
    ProtobufElement_ = std::move(element);
}

const NYson::TProtobufElement& TScalarAttributeSchema::GetProtobufElement() const
{
    return ProtobufElement_;
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeSchema::TFieldExpressionGetter MakeFieldExpressionGetter(const TDBField* field)
{
    return
        [=] (IQueryContext* context, const NYPath::TYPath& path, EAttributeExpressionContext) {
            return context->GetFieldExpression(field, path);
        };
}

NYTree::INodePtr GetAttributeDefaultValue(const TAttributeSchema* schema)
{
    if (auto* compositeSchema = schema->TryAsComposite()) {
        auto defaultValue = GetEphemeralNodeFactory()->CreateMap();
        for (const auto& [key, child] : compositeSchema->KeyToChild()) {
            YT_VERIFY(defaultValue->AddChild(key, GetAttributeDefaultValue(child)));
        }
        return defaultValue;
    } else if (auto* scalarSchema = schema->TryAsScalar(); scalarSchema && scalarSchema->HasDefaultValueGetter()) {
        return scalarSchema->RunDefaultValueGetter();
    } else {
        THROW_ERROR_EXCEPTION("Schema %Qv has no default value getter",
            schema->GetName());
    }
}

EAttributeType ResolveAttributeTypeByProtobufElement(const TProtobufElement& element, TYPathBuf path)
{
    return std::visit(TProtobufElementTypeResolver(path), element);
}

////////////////////////////////////////////////////////////////////////////////

void TAttributeSchema::ForEachVisibleLeafAttribute(
    const std::function<void(const TAttributeSchema*)>& callback) const
{
    if (auto* compositeSchema = TryAsComposite()) {
        for (const auto& [_, etcChild] : compositeSchema->NameToEtcChild()) {
            if (!etcChild->IsOpaque() && !etcChild->IsControl()) {
                etcChild->ForEachVisibleLeafAttribute(callback);
            }
        }
        for (const auto& [key, child] : compositeSchema->KeyToChild()) {
            if (!child->IsOpaque() && !child->IsControl()) {
                child->ForEachVisibleLeafAttribute(callback);
            }
        }
    } else {
        return callback(this);
    }
}

void TAttributeSchema::ValidateUpdatableCompositeAttributeNotSupported() const
{
    THROW_ERROR_EXCEPTION_IF(TryAsComposite() && (UpdatablePaths_.size() + SuperuserOnlyUpdatablePaths_.size()) > 0,
        "Updatable annotation for composite %v is not supported: set updatable annotation for all child attributes instead",
        FormatTypePath());
}

////////////////////////////////////////////////////////////////////////////////

TCompositeAttributeSchema::TCompositeAttributeSchema(
    IObjectTypeHandler* typeHandler,
    TObjectManager* objectManager,
    std::string name)
    : TAttributeSchema(typeHandler, objectManager, std::move(name))
{ }

void TCompositeAttributeSchema::AddChild(TAttributeSchema* child)
{
    SetParent(child, this);
    if (auto* scalarChild = child->TryAsScalar(); scalarChild && scalarChild->IsEtc()) {
        EmplaceOrCrash(NameToEtcChild_, scalarChild->GetName(), scalarChild);
        for (const auto& field : scalarChild->GetNestedFieldsMap()) {
            AddEtcChildField(scalarChild, field);
        }
    } else {
        EmplaceOrCrash(KeyToChild_, child->GetName(), child);
    }
}

TCompositeAttributeSchema* TCompositeAttributeSchema::AddChildren(const std::vector<TAttributeSchema*>& children)
{
    for (auto* child : children) {
        AddChild(child);
    }
    return this;
}

void TCompositeAttributeSchema::AddEtcChildField(TScalarAttributeSchema* etcChild, std::string_view field)
{
    auto it = KeyToEtcChild_.emplace(field, etcChild);
    YT_VERIFY(it.second || it.first->second == etcChild);
}

const TAttributeSchema* TCompositeAttributeSchema::FindChild(std::string_view key) const
{
    auto it = KeyToChild_.find(key);
    return it == KeyToChild_.end() ? nullptr : it->second.get();
}

TAttributeSchema* TCompositeAttributeSchema::FindChild(std::string_view key)
{

    auto it = KeyToChild_.find(key);
    return it == KeyToChild_.end() ? nullptr : it->second.get();
}

const TAttributeSchema* TCompositeAttributeSchema::GetChild(std::string_view key) const
{
    return GetOrCrash(KeyToChild_, key).get();
}

TAttributeSchema* TCompositeAttributeSchema::GetChild(std::string_view key)
{
    return GetOrCrash(KeyToChild_, key).get();
}

const TScalarAttributeSchema* TCompositeAttributeSchema::FindEtcChildByFieldName(std::string_view key) const
{
    auto it = KeyToEtcChild_.find(key);
    return it == KeyToEtcChild_.end() ? nullptr : it->second.get();
}

TScalarAttributeSchema* TCompositeAttributeSchema::FindEtcChildByFieldName(std::string_view key)
{
    auto it = KeyToEtcChild_.find(key);
    return it == KeyToEtcChild_.end() ? nullptr : it->second.get();
}

const TScalarAttributeSchema* TCompositeAttributeSchema::GetEtcChild(std::string_view name) const
{
    auto it = NameToEtcChild_.find(name);
    return it == NameToEtcChild_.end() ? nullptr : it->second;
}

TScalarAttributeSchema* TCompositeAttributeSchema::GetEtcChild(std::string_view name)
{
    auto it = NameToEtcChild_.find(name);
    return it == NameToEtcChild_.end() ? nullptr : it->second;
}

const TCompositeAttributeSchema::TKeyToChild<TAttributeSchema>& TCompositeAttributeSchema::KeyToChild() const
{
    return KeyToChild_;
}

TCompositeAttributeSchema::TKeyToChild<TAttributeSchema>& TCompositeAttributeSchema::KeyToChild()
{
    return KeyToChild_;
}

const TCompactFlatMap<std::string, TScalarAttributeSchema*, 4>& TCompositeAttributeSchema::NameToEtcChild() const
{
    return NameToEtcChild_;
}

void TCompositeAttributeSchema::ForEachImmediateChild(
    const std::function<void(const TAttributeSchema*)>& callback) const
{
    for (const auto& [key, childAttribute] : KeyToChild()) {
        callback(childAttribute);
    }

    for (const auto& [_, etcChild] : NameToEtcChild_) {
        callback(etcChild);
    }
}

TCompositeAttributeSchema* TCompositeAttributeSchema::SetUpdateMode(ESetUpdateObjectMode mode)
{
    UpdateMode_ = mode;
    return this;
}

std::optional<ESetUpdateObjectMode> TCompositeAttributeSchema::GetUpdateMode() const
{
    return UpdateMode_;
}

////////////////////////////////////////////////////////////////////////////////

void TMetaAttributeSchema::DisableEtcFieldToMetaResponse(std::string_view name)
{
    YT_VERIFY(KeyToEtcChild_.contains(name));
    DisabledEtcFieldPathsToMetaResponse_.push_back("/" + std::string(name));
}

const std::vector<TYPath>& TMetaAttributeSchema::GetDisabledEtcFieldPathsToMetaResponse() const
{
    return DisabledEtcFieldPathsToMetaResponse_;
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeSchema::TScalarAttributeSchema(
    IObjectTypeHandler* typeHandler,
    TObjectManager* objectManager,
    std::string name)
    : TAttributeSchema(typeHandler, objectManager, std::move(name))
{ }

TScalarAttributeSchema* TScalarAttributeSchema::SetEtc()
{
    Etc_ = true;
    YT_VERIFY(!IsOpaque());
    return this;
}

bool TScalarAttributeSchema::IsEtc() const
{
    return Etc_;
}

TScalarAttributeSchema* TScalarAttributeSchema::SetConstantChangedGetter(bool result)
{
    if (result) {
        ChangedGetterType_ = EChangedGetterType::ConstantTrue;
    } else {
        ChangedGetterType_ = EChangedGetterType::ConstantFalse;
    }

    return this;
}

bool TScalarAttributeSchema::HasChangedGetter() const
{
    return ChangedGetterType_ != EChangedGetterType::None;
}

EChangedGetterType TScalarAttributeSchema::GetChangedGetterType() const
{
    return ChangedGetterType_;
}

bool TScalarAttributeSchema::RunChangedGetter(const TObject* object, const TYPath& path) const
{
    switch (ChangedGetterType_) {
        case EChangedGetterType::ConstantFalse:
            return false;
        case EChangedGetterType::ConstantTrue:
            return true;
        case EChangedGetterType::Default:
        case EChangedGetterType::Custom:
            return ChangedGetter_(object, path);
        case EChangedGetterType::None:
        default:
            YT_ABORT();
    }
}

EChangedGetterType TScalarAttributeSchema::GetFilteredChangedGetterType() const
{
    return FilteredChangedGetterType_;
}

bool TScalarAttributeSchema::RunFilteredChangedGetter(
    const TObject* object,
    const TYPath& path,
    std::function<bool(const NYPath::TYPath&)> pathFilter) const
{
    if (HasFilteredChangedGetter()) {
        return FilteredChangedGetter_(object, path, pathFilter);
    }
    return false;
}

bool TScalarAttributeSchema::HasFilteredChangedGetter() const
{
    return FilteredChangedGetterType_ != EChangedGetterType::None;
}

TScalarAttributeSchema* TScalarAttributeSchema::SetValueSetter(std::function<void(
    TTransaction*,
    TObject*,
    const NYPath::TYPath&,
    const NYTree::INodePtr&,
    bool recursive,
    std::optional<bool> sharedWrite,
    EAggregateMode aggregateMode,
    const TTransactionCallContext&)> setter)
{
    ValueSetter_ = std::move(setter);
    return this;
}

bool TScalarAttributeSchema::HasValueSetter() const
{
    return ValueSetter_.operator bool();
}

void TScalarAttributeSchema::RunValueSetter(
    TTransaction* transaction,
    TObject* object,
    const TYPath& path,
    const INodePtr& value,
    bool recursive,
    std::optional<bool> sharedWrite,
    EAggregateMode aggregateMode,
    const TTransactionCallContext& transactionCallContext) const
{
    ValueSetter_(transaction, object, path, value, recursive, sharedWrite, aggregateMode, transactionCallContext);
}

bool TScalarAttributeSchema::HasLocker() const
{
    return Locker_.operator bool();
}

void TScalarAttributeSchema::RunLocker(
    TTransaction* transaction,
    TObject* object,
    const TYPath& path,
    NTableClient::ELockType lockType) const
{
    Locker_(transaction, object, path, lockType);
}

bool TScalarAttributeSchema::HasRemover() const
{
    return Remover_.operator bool();
}

void TScalarAttributeSchema::RunRemover(
    TTransaction* transaction,
    TObject* object,
    const TYPath& path,
    bool force) const
{
    Remover_(transaction, object, path, force);
}

bool TScalarAttributeSchema::HasMethod() const
{
    return MethodEvaluator_.operator bool();
}

void TScalarAttributeSchema::RunMethod(
    TTransaction* transaction,
    TObject* object,
    const NYPath::TYPath& path,
    const NYTree::INodePtr& value,
    NYson::IYsonConsumer* consumer) const
{
    MethodEvaluator_(transaction, object, path, value, consumer);
}

bool TScalarAttributeSchema::HasTimestampPregetter() const
{
    return TimestampPregetter_.operator bool();
}

void TScalarAttributeSchema::RunTimestampPregetter(
    const TObject* object,
    const TYPath& path) const
{
    TimestampPregetter_(object, path);
}

bool TScalarAttributeSchema::HasTimestampGetter() const
{
    return TimestampGetter_.operator bool();
}

TTimestamp TScalarAttributeSchema::RunTimestampGetter(
    const TObject* object,
    const TYPath& path) const
{
    return TimestampGetter_(object, path);
}

bool TScalarAttributeSchema::HasStoreScheduledGetter() const
{
    return StoreScheduledGetter_.operator bool();
}

bool TScalarAttributeSchema::RunStoreScheduledGetter(const TObject* object) const
{
    return StoreScheduledGetter_(object);
}

bool TScalarAttributeSchema::HasUpdatePreloader() const
{
    return UpdatePreloader_.operator bool();
}

void TScalarAttributeSchema::RunUpdatePreloader(
    TTransaction* transaction,
    const TObject* object,
    const TUpdateRequest& request) const
{
    UpdatePreloader_(transaction, object, request);
}

EValueGetterType TScalarAttributeSchema::GetValueGetterType() const
{
    return ValueGetterType_;
}

bool TScalarAttributeSchema::HasValueGetter() const
{
    bool hasValueGetter = ValueGetterType_ != EValueGetterType::None;
    YT_VERIFY(hasValueGetter == ValueGetter_.operator bool());
    return hasValueGetter;
}

void TScalarAttributeSchema::RunValueGetter(
    TTransaction* transaction,
    const TObject* object,
    NYson::IYsonConsumer* consumer,
    const TYPath& path) const
{
    ValueGetter_(transaction, object, consumer, path);
}

TScalarAttributeSchema* TScalarAttributeSchema::SetDefaultValueGetter(std::function<NYTree::INodePtr()> defaultValueGetter)
{
    DefaultValueGetter_ = std::move(defaultValueGetter);
    return this;
}

bool TScalarAttributeSchema::HasDefaultValueGetter() const
{
    return DefaultValueGetter_.operator bool();
}

INodePtr TScalarAttributeSchema::RunDefaultValueGetter() const
{
    return DefaultValueGetter_();
}

bool TScalarAttributeSchema::HasInitializer() const
{
    return Initializer_.operator bool();
}

void TScalarAttributeSchema::RunInitializer(
    TTransaction* transaction,
    TObject* object) const
{
    Initializer_(transaction, object);
}

TScalarAttributeSchema* TScalarAttributeSchema::SetExpressionBuilder(
    TPathValidator pathValidator,
    TFieldExpressionGetter fieldExpressionGetter)
{
    ExpressionBuilder_ =
        [pathValidator = std::move(pathValidator), fieldExpressionGetter = std::move(fieldExpressionGetter), this] (
            IQueryContext* context,
            const NYPath::TYPath& path,
            EAttributeExpressionContext expressionContext)
        {
            pathValidator(this, path);
            return fieldExpressionGetter(context, path, expressionContext);
        };
    return this;
}

TScalarAttributeSchema* TScalarAttributeSchema::SetExpressionBuilder(std::function<TExpressionPtr(IQueryContext*)> builder)
{
    ExpressionBuilder_ =
        [builder = std::move(builder), this] (IQueryContext* context, const TYPath& path, EAttributeExpressionContext) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Attribute %Qv can only be queried as a whole",
                FormatPathEtc());
            return builder(context);
        };
    return this;
}

bool TScalarAttributeSchema::HasExpressionBuilder() const
{
    return ExpressionBuilder_.operator bool();
}

TExpressionPtr TScalarAttributeSchema::RunExpressionBuilder(
    IQueryContext* context,
    const TYPath& path,
    EAttributeExpressionContext expressionContext) const
{
    return ExpressionBuilder_(context, path, expressionContext);
}

TScalarAttributeSchema* TScalarAttributeSchema::SetTimestampExpressionBuilder(
    std::function<NQueryClient::NAst::TExpressionPtr(IQueryContext*, const NYPath::TYPath&)> builder)
{
    TimestampExpressionBuilder_ = std::move(builder);
    return this;
}

bool TScalarAttributeSchema::HasTimestampExpressionBuilder() const
{
    return TimestampExpressionBuilder_.operator bool();
}

NQueryClient::NAst::TExpressionPtr TScalarAttributeSchema::RunTimestampExpressionBuilder(
    IQueryContext* context,
    const NYPath::TYPath& path) const
{
    return TimestampExpressionBuilder_(context, path);
}

TScalarAttributeSchema* TScalarAttributeSchema::SetIdAttribute(
    const TDBField* field,
    TValueGetter valueGetter)
{
    Field_ = field;
    IsIdAttribute_ = true;
    ForbidValidators_ = true;
    YT_VERIFY(Validators_.empty());
    ValueGetterType_ = EValueGetterType::Evaluator;
    ValueGetter_ = valueGetter;
    SetExpressionBuilder(
        ValidatePathIsEmpty,
        MakeFieldExpressionGetter(Field_));
    return this;
}

TScalarAttributeSchema* TScalarAttributeSchema::SetParentIdAttribute(
    const TDBField* field,
    TValueGetter valueGetter)
{
    Field_ = field;
    ForbidValidators_ = true;
    YT_VERIFY(Validators_.empty());
    ValueGetterType_ = EValueGetterType::Evaluator;
    ValueGetter_ = valueGetter;
    SetExpressionBuilder(
        ValidatePathIsEmpty,
        MakeFieldExpressionGetter(Field_));
    return this;
}

const TDBField* TScalarAttributeSchema::TryGetDBField() const
{
    return Field_;
}

const TDBField* TScalarAttributeSchema::GetDBField() const
{
    auto* field = TryGetDBField();
    THROW_ERROR_EXCEPTION_IF(!field,
        "Attribute %Qv has no DB field",
        FormatPathEtc());
    return field;
}

TAttributeSchema* TScalarAttributeSchema::SetCustomPolicyExpected()
{
    CustomPolicyExpected_ = true;
    return this;
}

void TScalarAttributeSchema::ValidateKeyField(
    const TObjectKey::TKeyField& keyField,
    std::string_view overrideTitle) const
{
    if (IsIdAttribute_) { // Parent keys do not get policies.
        KeyFieldValidator_(keyField, overrideTitle);
    }
}

void TScalarAttributeSchema::SetKeyFieldEvaluator(
    std::function<std::optional<TObjectKey::TKeyField>(NYTree::INodePtr)> evaluator)
{
    KeyFieldEvaluator_ = std::move(evaluator);
}

std::optional<TObjectKey::TKeyField> TScalarAttributeSchema::TryEvaluateKeyField(NYTree::INodePtr meta) const
{
    if (KeyFieldEvaluator_ && nullptr != meta) {
        return KeyFieldEvaluator_(meta);
    }
    return std::nullopt;
}

TObjectKey::TKeyField TScalarAttributeSchema::GenerateKeyField(TTransaction* transaction) const
{
    YT_VERIFY(KeyFieldGenerator_);
    return KeyFieldGenerator_(transaction);
}

TScalarAttributeSchema* TScalarAttributeSchema::SetAccessControlParentIdAttribute(const TDBField* field)
{
    Field_ = field;

    return this;
}

void TScalarAttributeSchema::Validate() const
{
    YT_VERIFY(!CustomPolicyExpected_ ||
        CustomPolicyExpected_ && (
            (IsIdAttribute_ && KeyFieldGenerator_ && KeyFieldValidator_) ||
            (!IsIdAttribute_ && Initializer_)));
}

bool TScalarAttributeSchema::IsAggregated() const
{
    return Aggregated_;
}

EAttributeType TScalarAttributeSchema::GetType(TYPathBuf path) const
{
    return ResolveAttributeTypeByProtobufElement(ProtobufElement_, path);
}

TScalarAttributeSchema* TScalarAttributeSchema::SetNestedFieldsMap(std::vector<std::string> fields)
{
    std::move(fields.begin(), fields.end(), std::back_inserter(NestedFieldsMap_));
    return this;
}

const std::vector<std::string>& TScalarAttributeSchema::GetNestedFieldsMap() const
{
    return NestedFieldsMap_;
}

std::vector<const TTagSet*> TScalarAttributeSchema::CollectChangedTags(const TObject* object) const
{
    std::vector<const TTagSet*> changedTags;
    YT_VERIFY(HasChangedGetter() || HasStoreScheduledGetter());
    if (HasStoreScheduledGetter() && !RunStoreScheduledGetter(object)) {
        return changedTags;
    }

    for (const auto& entry : TagsIndex_) {
        if (!HasChangedGetter() || RunChangedGetter(object, entry.Path)) {
            changedTags.push_back(&entry.Tags);
        }
    }
    return changedTags;
}

void TScalarAttributeSchema::SetTagsIndex(std::vector<TTagsIndexEntry> index)
{
    TagsIndex_ = std::move(index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
