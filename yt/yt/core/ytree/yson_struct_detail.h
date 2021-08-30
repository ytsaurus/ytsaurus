#pragma once

#include "yson_struct.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
using TYsonStructField = TValue(TStruct::*);

struct TLoadParameterOptions
{
    NYPath::TYPath Path;
    bool KeepUnrecognizedRecursively = false;
    std::optional<EMergeStrategy> MergeStrategy = std::nullopt;
};

////////////////////////////////////////////////////////////////////////////////

struct IYsonStructParameter
    : public TRefCounted
{
    virtual void Load(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options) = 0;
    virtual void SafeLoad(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options,
        const std::function<void()>& validate) = 0;

    virtual void Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const = 0;

    virtual void Postprocess(const TYsonStructBase* self, const NYPath::TYPath& path) const = 0;

    virtual void SetDefaultsUninitialized(TYsonStructBase* self) = 0;
    virtual void SetDefaultsInitialized(TYsonStructBase* self) = 0;

    virtual bool CanOmitValue(const TYsonStructBase* self) const = 0;

    virtual const TString& GetKey() const = 0;
    virtual const std::vector<TString>& GetAliases() const = 0;
    virtual IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* self) const = 0;
};

//using IYsonStructParameterPtr = TIntrusivePtr<IYsonStructParameter>;
DECLARE_REFCOUNTED_STRUCT(IYsonStructParameter);
DEFINE_REFCOUNTED_TYPE(IYsonStructParameter);

////////////////////////////////////////////////////////////////////////////////

struct IYsonStructMeta
{
    virtual const THashMap<TString, IYsonStructParameterPtr>& GetParameterMap() const = 0;
    virtual const std::vector<std::pair<TString, IYsonStructParameterPtr>>& GetParameterSortedList() const = 0;
    virtual void SetDefaultsOfInitializedStruct(TYsonStructBase* target) const = 0;
    virtual const THashSet<TString>& GetRegisteredKeys() const = 0;
    virtual void Postprocess(TYsonStructBase* target, const TYPath& path) const = 0;
    virtual IYsonStructParameterPtr GetParameter(const TString& keyOrAlias) const = 0;
    virtual void LoadParameter(TYsonStructBase* target, const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy) const = 0;

    virtual void LoadStruct(
        TYsonStructBase* target,
        INodePtr node,
        bool postprocess,
        bool setDefaults,
        const TYPath& path) const = 0;

    virtual IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* target) const = 0;

    //! Signal that we are about to initialize next class in hierarchy.
    /*! When we initialize meta and traverse through type hierarchy
     * we call this method before starting initialization of each class in hierarchy.
     */
    virtual void OnNextClassInHierarchy() = 0;

    virtual void RegisterParameter(TString key, IYsonStructParameterPtr parameter) = 0;
    virtual void RegisterPreprocessor(std::function<void(TYsonStructBase*)> preprocessor) = 0;
    virtual void RegisterPostprocessor(std::function<void(TYsonStructBase*)> postprocessor) = 0;
    virtual void SetUnrecognizedStrategy(EUnrecognizedStrategy strategy) = 0;

    virtual ~IYsonStructMeta() = default;
};

///////////////////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
class TYsonStructMeta
    : public IYsonStructMeta
{
public:
    void SetTopLevelDefaultsOfUninitializedStruct(TYsonStructLite* target) const;
    virtual void SetDefaultsOfInitializedStruct(TYsonStructBase* target) const override;

    virtual const THashMap<TString, IYsonStructParameterPtr>& GetParameterMap() const override;
    virtual const std::vector<std::pair<TString, IYsonStructParameterPtr>>& GetParameterSortedList() const override;
    virtual const THashSet<TString>& GetRegisteredKeys() const override;

    virtual IYsonStructParameterPtr GetParameter(const TString& keyOrAlias) const;
    virtual void LoadParameter(TYsonStructBase* target, const TString& key, const NYTree::INodePtr& node, EMergeStrategy mergeStrategy) const override;

    virtual void Postprocess(TYsonStructBase* target, const TYPath& path) const override;

    virtual void LoadStruct(
        TYsonStructBase* target,
        INodePtr node,
        bool postprocess,
        bool setDefaults,
        const TYPath& path) const;

    virtual IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* target) const override;

    virtual void RegisterParameter(TString key, IYsonStructParameterPtr parameter);
    virtual void RegisterPreprocessor(std::function<void(TYsonStructBase*)> preprocessor) override;
    virtual void RegisterPostprocessor(std::function<void(TYsonStructBase*)> postprocessor) override;
    virtual void SetUnrecognizedStrategy(EUnrecognizedStrategy strategy) override;

    virtual void OnNextClassInHierarchy() override;

    void FinishInitialization();

private:
    friend class TYsonStructRegistry;

    THashMap<TString, IYsonStructParameterPtr> Parameters_;
    std::vector<std::pair<TString, IYsonStructParameterPtr>> SortedParameters_;
    // It contains only those parameters that are registered just in TStruct (without ancestor's parameters).
    THashMap<TString, IYsonStructParameterPtr> TopLevelParameters_;
    THashSet<TString> RegisteredKeys_;

    std::vector<std::function<void(TYsonStructBase*)>> Preprocessors_;
    // Same as `TopLevelParameters`.
    std::vector<std::function<void(TYsonStructBase*)>> TopLevelPreprocessors_;
    std::vector<std::function<void(TYsonStructBase*)>> Postprocessors_;

    EUnrecognizedStrategy MetaUnrecognizedStrategy_;
};

////////////////////////////////////////////////////////////////////////////////

//! Type erasing interface.
/*! This interface and underlying class is used to erase TStruct type parameter from TYsonStructParameter.
 * Otherwise we would have TYsonStructParameter<TStruct, TValue>
 * and compiler would have to instantiate huge template for each pair <TStruct, TValue>.
 */
template <class TValue>
struct IYsonFieldAccessor
{
    virtual TValue& GetValue(const TYsonStructBase* source, bool useDynamicCastCache = true) = 0;
    virtual ~IYsonFieldAccessor() = default;
};

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
class TYsonFieldAccessor
    : public IYsonFieldAccessor<TValue>
{
public:
    explicit TYsonFieldAccessor(TYsonStructField<TStruct, TValue> field);
    virtual TValue& GetValue(const TYsonStructBase* source, bool useDynamicCastCache) override;

private:
    TYsonStructField<TStruct, TValue> Field_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TYsonStructParameter
    : public IYsonStructParameter
{
public:
    using TPostprocessor = std::function<void(const TValue&)>;
    using TValueType = typename TOptionalTraits<TValue>::TValue;

    TYsonStructParameter(
        TString key,
        std::unique_ptr<IYsonFieldAccessor<TValue>> fieldAccessor);

    virtual void Load(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options) override;
    virtual void SafeLoad(
        TYsonStructBase* self,
        NYTree::INodePtr node,
        const TLoadParameterOptions& options,
        const std::function<void()>& validate) override;
    virtual void Postprocess(const TYsonStructBase* self, const NYPath::TYPath& path) const override;
    virtual void SetDefaultsUninitialized(TYsonStructBase* self) override;
    virtual void SetDefaultsInitialized(TYsonStructBase* self) override;
    virtual void Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const override;
    virtual bool CanOmitValue(const TYsonStructBase* self) const override;
    virtual const TString& GetKey() const override;
    virtual const std::vector<TString>& GetAliases() const override;
    virtual IMapNodePtr GetRecursiveUnrecognized(const TYsonStructBase* self) const override;

    // Mark as optional.
    TYsonStructParameter& Optional();
    // Set default value. It will be copied during instance initialization.
    TYsonStructParameter& Default(TValue defaultValue = TValue());
    // Register constructor for default value. It will be called during instance initialization.
    TYsonStructParameter& DefaultCtor(std::function<TValue()> defaultCtor);
    // Omit this parameter during serialization if it is equal to default.
    TYsonStructParameter& DontSerializeDefault();
    // Register general purpose validator for parameter. Used by other validators.
    // It is called after deserialization.
    TYsonStructParameter& CheckThat(TPostprocessor validator);
    // Register validator that checks value to be greater than given value.
    TYsonStructParameter& GreaterThan(TValueType value);
    // Register validator that checks value to be greater than or equal to given value.
    TYsonStructParameter& GreaterThanOrEqual(TValueType value);
    // Register validator that checks value to be less than given value.
    TYsonStructParameter& LessThan(TValueType value);
    // Register validator that checks value to be less than or equal to given value.
    TYsonStructParameter& LessThanOrEqual(TValueType value);
    // Register validator that checks value to be in given range.
    TYsonStructParameter& InRange(TValueType lowerBound, TValueType upperBound);
    // Register validator that checks value to be non empty.
    TYsonStructParameter& NonEmpty();
    // Register alias for parameter. Used in deserialization.
    TYsonStructParameter& Alias(const TString& name);
    // Set merge strategy for parameter
    TYsonStructParameter& MergeBy(EMergeStrategy strategy);

    // Register constructor with parameters as initializer of default value for ref-counted class.
    template <class... TArgs>
    TYsonStructParameter& DefaultNew(TArgs&&... args);

private:
    const TString Key_;
    std::unique_ptr<IYsonFieldAccessor<TValue>> FieldAccessor_;
    std::optional<std::function<TValue()>> DefaultConstructor_;
    bool SerializeDefault_ = true;
    std::vector<TPostprocessor> Postprocessors_;
    std::vector<TString> Aliases_;
    EMergeStrategy MergeStrategy_ = EMergeStrategy::Default;
    bool IsTriviallyInitializedIntrusivePtr_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YSON_STRUCT_DETAIL_INL_H_
#include "yson_struct_detail-inl.h"
#undef YSON_STRUCT_DETAIL_INL_H_
