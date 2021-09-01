#pragma once
#ifndef YSON_STRUCT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct_detail.h"
// For the sake of sane code completion.
#include "yson_struct_detail.h"
#endif

#include "ypath_client.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <class T>
void LoadFromNode(
    T& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy /*mergeStrategy*/,
    bool /*keepUnrecognizedRecursively*/)
{
    try {
        Deserialize(parameter, node);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", path)
                << ex;
    }
}

// INodePtr
template <>
inline void LoadFromNode(
    NYTree::INodePtr& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& /*path*/,
    EMergeStrategy mergeStrategy,
    bool /*keepUnrecognizedRecursively*/)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            parameter = node;
            break;
        }

        case EMergeStrategy::Combine: {
            if (!parameter) {
                parameter = node;
            } else {
                parameter = PatchNode(parameter, node);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// TYsonStruct
template <class T, class E = typename std::enable_if<std::is_convertible<T&, TYsonStruct&>::value>::type>
void LoadFromNode(
    TIntrusivePtr<T>& parameterValue,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    if (!parameterValue || mergeStrategy == EMergeStrategy::Overwrite) {
        parameterValue = New<T>();
    }

    if (keepUnrecognizedRecursively) {
        parameterValue->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameterValue->Load(node, false, false, path);
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::optional
template <class T>
void LoadFromNode(
    std::optional<T>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            if (node->GetType() == NYTree::ENodeType::Entity) {
                parameter = std::nullopt;
            } else {
                T value;
                LoadFromNode(value, node, path, EMergeStrategy::Overwrite, keepUnrecognizedRecursively);
                parameter = value;
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::vector
template <class... T>
void LoadFromNode(
    std::vector<T...>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            auto listNode = node->AsList();
            auto size = listNode->GetChildCount();
            parameter.resize(size);
            for (int i = 0; i < size; ++i) {
                LoadFromNode(
                    parameter[i],
                    listNode->GetChildOrThrow(i),
                    path + "/" + NYPath::ToYPathLiteral(i),
                    EMergeStrategy::Overwrite,
                    keepUnrecognizedRecursively);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

template <class T>
T DeserializeMapKey(TStringBuf value)
{
    if constexpr (TEnumTraits<T>::IsEnum) {
        return TEnumTraits<T>::FromString(DecodeEnumValue(value));
    } else {
        return FromString<T>(value);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class M = typename Map<T...>::mapped_type>
void LoadFromNode(
    Map<T...>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            auto mapNode = node->AsMap();
            parameter.clear();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                M value;
                LoadFromNode(
                    value,
                    child,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Overwrite,
                    keepUnrecognizedRecursively);
                parameter.emplace(DeserializeMapKey<typename Map<T...>::key_type>(key), std::move(value));
            }
            break;
        }
        case EMergeStrategy::Combine: {
            auto mapNode = node->AsMap();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                M value;
                LoadFromNode(
                    value,
                    child,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Combine,
                    keepUnrecognizedRecursively);
                parameter[DeserializeMapKey<typename Map<T...>::key_type>(key)] = std::move(value);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// For all classes except descendants of TYsonStructBase and their intrusive pointers
// we do not attempt to extract unrecognized members. C++ prohibits function template specialization
// so we have to deal with static struct members.
template <class T, class = void>
struct TGetRecursiveUnrecognized
{
    static IMapNodePtr Do(const T& /*parameter*/)
    {
        return nullptr;
    }
};

template <class T>
struct TGetRecursiveUnrecognized<T, std::enable_if_t<std::is_base_of<TYsonStructBase, T>::value>>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter.GetRecursiveUnrecognized();
    }
};

template <class T>
struct TGetRecursiveUnrecognized<T, std::enable_if_t<std::is_base_of<TYsonStructBase, typename T::TUnderlying>::value>>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter ? parameter->GetRecursiveUnrecognized() : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /* parameter */,
    const NYPath::TYPath& /* path */,
    const F& /* func */)
{ }

// TYsonStruct
template <class T, class F, class E = typename std::enable_if<std::is_convertible<T*, TYsonStruct*>::value>::type>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    func(*parameter, path);
}

// std::vector
template <class... T, class F>
inline void InvokeForComposites(
    const std::vector<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (size_t i = 0; i < parameter->size(); ++i) {
        InvokeForComposites(
            &(*parameter)[i],
            path + "/" + NYPath::ToYPathLiteral(i),
            func);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(
    const Map<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(
            &value,
            path + "/" + NYPath::ToYPathLiteral(key),
            func);
    }
}

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /* parameter */,
    const F& /* func */)
{ }

// TYsonStruct
template <class T, class F, class E = typename std::enable_if<std::is_convertible<T*, TYsonStruct*>::value>::type>
inline void InvokeForComposites(const TIntrusivePtr<T>* parameter, const F& func)
{
    func(*parameter);
}

// std::vector
template <class... T, class F>
inline void InvokeForComposites(const std::vector<T...>* parameter, const F& func)
{
    for (const auto& item : *parameter) {
        InvokeForComposites(&item, func);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(const Map<T...>* parameter, const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(&value, func);
    }
}

template <class T, class = void>
struct IsYsonStructPtr : std::false_type
{ };

template <class T>
struct IsYsonStructPtr<TIntrusivePtr<T>, typename std::enable_if<std::is_convertible<T&, TYsonStruct&>::value>::type> : std::true_type
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYsonStructDetail

////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
void TYsonStructMeta<TStruct>::SetDefaultsOfInitializedStruct(TYsonStructBase* target) const
{
    for (const auto& [_, parameter] : Parameters_) {
        parameter->SetDefaultsInitialized(target);
    }

    for (const auto& preprocessor : Preprocessors_) {
        preprocessor(target);
    }
}

template <class TStruct>
const THashSet<TString>& TYsonStructMeta<TStruct>::GetRegisteredKeys() const
{
    return RegisteredKeys_;
}

template <class TStruct>
const THashMap<TString, IYsonStructParameterPtr>& TYsonStructMeta<TStruct>::GetParameterMap() const
{
    return Parameters_;
}

template <class TStruct>
const std::vector<std::pair<TString, IYsonStructParameterPtr>>& TYsonStructMeta<TStruct>::GetParameterSortedList() const
{
    return SortedParameters_;
}

template <class TStruct>
IYsonStructParameterPtr TYsonStructMeta<TStruct>::GetParameter(const TString& keyOrAlias) const
{
    auto it = Parameters_.find(keyOrAlias);
    if (it != Parameters_.end()) {
        return it->second;
    }

    for (const auto& [_, parameter] : Parameters_) {
        if (Count(parameter->GetAliases(), keyOrAlias) > 0) {
            return parameter;
        }
    }
    THROW_ERROR_EXCEPTION("Key or alias %Qv not found in yson struct", keyOrAlias);
}

template <class TStruct>
void TYsonStructMeta<TStruct>::LoadParameter(TYsonStructBase* target, const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy) const
{
    const auto& parameter = GetParameter(key);
    auto validate = [&] () {
        parameter->Postprocess(target, "/" + key);
        try {
            for (const auto& postprocessor : Postprocessors_) {
                postprocessor(target);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Postprocess failed while loading parameter %Qv from value %Qv",
                key,
                ConvertToYsonString(node, NYson::EYsonFormat::Text))
                    << ex;
        }
    };
    auto loadOptions = TLoadParameterOptions{
        .Path = "",
        .MergeStrategy = mergeStrategy
    };

    parameter->SafeLoad(target, node, loadOptions, validate);
}

template <class TStruct>
void TYsonStructMeta<TStruct>::Postprocess(TYsonStructBase* target, const TYPath& path) const
{
    for (const auto& [name, parameter] : Parameters_) {
        parameter->Postprocess(target, path + "/" + name);
    }

    try {
        for (const auto& postprocessor : Postprocessors_) {
            postprocessor(target);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Postprocess failed at %v",
            path.empty() ? "root" : path)
                << ex;
    }
}

template <class TStruct>
void TYsonStructMeta<TStruct>::LoadStruct(
    TYsonStructBase* target,
    INodePtr node,
    bool postprocess,
    bool setDefaults,
    const TYPath& path) const
{
    YT_VERIFY(node);

    if (setDefaults) {
        SetDefaultsOfInitializedStruct(target);
    }

    auto mapNode = node->AsMap();
    auto unrecognizedStrategy = target->InstanceUnrecognizedStrategy_.template value_or(MetaUnrecognizedStrategy_);
    for (const auto& [name, parameter] : Parameters_) {
        TString key = name;
        auto child = mapNode->FindChild(name); // can be NULL
        for (const auto& alias : parameter->GetAliases()) {
            auto otherChild = mapNode->FindChild(alias);
            if (child && otherChild && !AreNodesEqual(child, otherChild)) {
                THROW_ERROR_EXCEPTION("Different values for aliased parameters %Qv and %Qv", key, alias)
                        << TErrorAttribute("main_value", child)
                            << TErrorAttribute("aliased_value", otherChild);
            }
            if (!child && otherChild) {
                child = otherChild;
                key = alias;
            }
        }
        auto loadOptions = TLoadParameterOptions{
            .Path = path + "/" + key,
            .KeepUnrecognizedRecursively = unrecognizedStrategy == EUnrecognizedStrategy::KeepRecursive
        };
        parameter->Load(target, child, loadOptions);
    }

    if (unrecognizedStrategy != EUnrecognizedStrategy::Drop) {
        auto registeredKeys = GetRegisteredKeys();
        if (!target->LocalUnrecognized_) {
            target->LocalUnrecognized_ = GetEphemeralNodeFactory()->CreateMap();
        }
        for (const auto& [key, child] : mapNode->GetChildren()) {
            if (registeredKeys.find(key) == registeredKeys.end()) {
                target->LocalUnrecognized_->RemoveChild(key);
                YT_VERIFY(target->LocalUnrecognized_->AddChild(key, ConvertToNode(child)));
            }
        }
    }

    if (postprocess) {
        Postprocess(target, path);
    }
}

template <class TStruct>
IMapNodePtr TYsonStructMeta<TStruct>::GetRecursiveUnrecognized(const TYsonStructBase* target) const
{
    // Take a copy of `LocalUnrecognized` and add parameter->GetRecursiveUnrecognized()
    // for all parameters that are TYsonStruct's themselves.
    auto result = target->LocalUnrecognized_ ? ConvertTo<IMapNodePtr>(target->LocalUnrecognized_) : GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [name, parameter] : Parameters_) {
        auto unrecognized = parameter->GetRecursiveUnrecognized(target);
        if (unrecognized && unrecognized->AsMap()->GetChildCount() > 0) {
            result->AddChild(name, unrecognized);
        }
    }
    return result;
}

template <class TStruct>
void TYsonStructMeta<TStruct>::RegisterParameter(TString key, IYsonStructParameterPtr parameter)
{
    YT_VERIFY(Parameters_.template emplace(std::move(key), std::move(parameter)).second);
}

template <class TStruct>
void TYsonStructMeta<TStruct>::RegisterPreprocessor(std::function<void(TYsonStructBase*)> preprocessor)
{
    Preprocessors_.push_back(std::move(preprocessor));
}

template <class TStruct>
void TYsonStructMeta<TStruct>::RegisterPostprocessor(std::function<void(TYsonStructBase*)> postprocessor)
{
    Postprocessors_.push_back(std::move(postprocessor));
}

template <class TStruct>
void TYsonStructMeta<TStruct>::SetUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    MetaUnrecognizedStrategy_ = strategy;
}

template <class TStruct>
void TYsonStructMeta<TStruct>::FinishInitialization()
{
    RegisteredKeys_.reserve(Parameters_.size());
    for (const auto& [name, parameter] : Parameters_) {
        RegisteredKeys_.insert(name);
        for (const auto& alias : parameter->GetAliases()) {
            RegisteredKeys_.insert(alias);
        }
    }

    SortedParameters_ = std::vector<std::pair<TString, IYsonStructParameterPtr>>(Parameters_.begin(), Parameters_.end());
    std::sort(
        SortedParameters_.begin(),
        SortedParameters_.end(),
        [] (const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
}

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
TYsonFieldAccessor<TStruct, TValue>::TYsonFieldAccessor(TYsonStructField<TStruct, TValue> field)
    : Field_(field)
{ };

template <class TStruct, class TValue>
TValue& TYsonFieldAccessor<TStruct, TValue>::GetValue(const TYsonStructBase* source)
{
    return TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(source)->*Field_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TYsonStructParameter<TValue>::TYsonStructParameter(TString key, std::unique_ptr<IYsonFieldAccessor<TValue>> fieldAccessor)
    : Key_(std::move(key))
    , FieldAccessor_(std::move(fieldAccessor))
    , MergeStrategy_(EMergeStrategy::Default)
{ }

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options)
{
    if (node) {
        NPrivate::LoadFromNode(
            FieldAccessor_->GetValue(self),
            std::move(node),
            options.Path,
            options.MergeStrategy.value_or(MergeStrategy_),
            options.KeepUnrecognizedRecursively);
    } else if (!DefaultConstructor_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.Path);
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::SafeLoad(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options,
    const std::function<void()>& validate)
{
    if (node) {
        TValue oldValue = FieldAccessor_->GetValue(self);
        try {
            NPrivate::LoadFromNode(FieldAccessor_->GetValue(self), node, options.Path, options.MergeStrategy.value_or(MergeStrategy_), false);
            validate();
        } catch (const std::exception ex) {
            FieldAccessor_->GetValue(self) = oldValue;
            throw;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Postprocess(const TYsonStructBase* self, const NYPath::TYPath& path) const
{
    const auto& value = FieldAccessor_->GetValue(self);
    for (const auto& postprocessor : Postprocessors_) {
        try {
            postprocessor(value);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Postprocess failed at %v",
                path.empty() ? "root" : path)
                    << ex;
        }
    }

    NPrivate::InvokeForComposites(
        &value,
        path,
        [] (TIntrusivePtr<TYsonStruct> obj, const NYPath::TYPath& subpath) {
            if (obj) {
                obj->Postprocess(subpath);
            }
        });
}

template <class TValue>
void TYsonStructParameter<TValue>::SetDefaultsInitialized(TYsonStructBase* self)
{
    TValue& value = FieldAccessor_->GetValue(self);

    if (DefaultConstructor_) {
        value = (*DefaultConstructor_)();
    }

    NPrivate::InvokeForComposites(
        &value,
        [] (TIntrusivePtr<TYsonStruct> obj) {
            if (obj) {
                obj->SetDefaults();
            }
        });
}

template <class TValue>
void TYsonStructParameter<TValue>::Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const
{
    using NYTree::Serialize;
    Serialize(FieldAccessor_->GetValue(self), consumer);
}

template <class TValue>
bool TYsonStructParameter<TValue>::CanOmitValue(const TYsonStructBase* self) const
{
    const auto& value = FieldAccessor_->GetValue(self);
    if constexpr (std::is_arithmetic_v<TValue> || std::is_same_v<TValue, TString>) {
        if (!SerializeDefault_ && value == (*DefaultConstructor_)()) {
            return true;
        }
    }

    if (!DefaultConstructor_) {
        return NYT::NYTree::NDetail::CanOmitValue(&value, nullptr);
    }

    if (IsTriviallyInitializedIntrusivePtr_) {
        return false;
    }

    auto defaultValue = (*DefaultConstructor_)();
    return NYT::NYTree::NDetail::CanOmitValue(&value, &defaultValue);
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Alias(const TString& name)
{
    Aliases_.push_back(name);
    return *this;
}

template <class TValue>
const std::vector<TString>& TYsonStructParameter<TValue>::GetAliases() const
{
    return Aliases_;
}

template <class TValue>
const TString& TYsonStructParameter<TValue>::GetKey() const
{
    return Key_;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Optional()
{
    DefaultConstructor_ = [] () { return TValue{}; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default(TValue defaultValue)
{
    DefaultConstructor_ = [value = std::move(defaultValue)]() { return value; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultCtor(std::function<TValue()> defaultCtor)
{
    DefaultConstructor_ = std::move(defaultCtor);
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DontSerializeDefault()
{
    // We should check for equality-comparability here but it is rather hard
    // to do the deep validation.
    static_assert(
        std::is_arithmetic_v<TValue> || std::is_same_v<TValue, TString> || std::is_same_v<TValue, std::optional<TString>>,
        "DontSerializeDefault requires |Parameter| to be TString or arithmetic type");

    SerializeDefault_ = false;
    return *this;
}

template <class TValue>
template <class... TArgs>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultNew(TArgs&&... args)
{
    IsTriviallyInitializedIntrusivePtr_ = true;
    return DefaultCtor([=] () { return New<typename TValue::TUnderlying>(std::forward<TArgs>(args)...); });
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::CheckThat(TPostprocessor postprocessor)
{
    Postprocessors_.push_back(std::move(postprocessor));
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::MergeBy(EMergeStrategy strategy)
{
    MergeStrategy_ = strategy;
    return *this;
}

template <class TValue>
IMapNodePtr TYsonStructParameter<TValue>::GetRecursiveUnrecognized(const TYsonStructBase* self) const
{
    return NPrivate::TGetRecursiveUnrecognized<TValue>::Do(FieldAccessor_->GetValue(self));
}

////////////////////////////////////////////////////////////////////////////////
// Standard postprocessors

#define DEFINE_POSTPROCESSOR(method, condition, error) \
    template <class TValue> \
    TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::method \
    { \
        return CheckThat([=] (const TValue& parameter) { \
            using ::ToString; \
            std::optional<TValueType> nullableParameter(parameter); \
            if (nullableParameter) { \
                const auto& actual = *nullableParameter; \
                if (!(condition)) { \
                    THROW_ERROR error; \
                } \
            } \
        }); \
    }

DEFINE_POSTPROCESSOR(
    GreaterThan(TValueType expected),
    actual > expected,
    TError("Expected > %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    GreaterThanOrEqual(TValueType expected),
    actual >= expected,
    TError("Expected >= %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    LessThan(TValueType expected),
    actual < expected,
    TError("Expected < %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    LessThanOrEqual(TValueType expected),
    actual <= expected,
    TError("Expected <= %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    InRange(TValueType lowerBound, TValueType upperBound),
    lowerBound <= actual && actual <= upperBound,
    TError("Expected in range [%v,%v], found %v", lowerBound, upperBound, actual)
)

DEFINE_POSTPROCESSOR(
    NonEmpty(),
    actual.size() > 0,
    TError("Value must not be empty")
)

#undef DEFINE_POSTPROCESSOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
