#pragma once

#include "public.h"
#include "node.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/mpl.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

#include <yt/core/yson/public.h>

#include <functional>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMergeStrategy,
    (Default)
    (Overwrite)
    (Combine)
);

DEFINE_ENUM(EUnrecognizedStrategy,
    (Drop)
    (Keep)
    (KeepRecursive)
);

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializableLite
    : private TNonCopyable
{
public:
    typedef std::function<void()> TPostprocessor;
    typedef std::function<void()> TPreprocessor;

    struct IParameter
        : public TIntrinsicRefCounted
    {
        virtual void Load(NYTree::INodePtr node, const NYPath::TYPath& path) = 0;
        virtual void Postprocess(const NYPath::TYPath& path) const = 0;
        virtual void SetDefaults() = 0;
        virtual void Save(NYson::IYsonConsumer* consumer) const = 0;
        virtual bool CanOmitValue() const = 0;
        virtual const std::vector<TString>& GetAliases() const = 0;
        virtual IMapNodePtr GetUnrecognizedRecursively() const = 0;
        virtual void SetKeepUnrecognizedRecursively() = 0;
    };

    typedef TIntrusivePtr<IParameter> IParameterPtr;

    template <class T>
    class TParameter
        : public IParameter
    {
    public:
        typedef std::function<void(const T&)> TPostprocessor;
        typedef typename TNullableTraits<T>::TValueType TValueType;

        explicit TParameter(T& parameter);

        virtual void Load(NYTree::INodePtr node, const NYPath::TYPath& path) override;
        virtual void Postprocess(const NYPath::TYPath& path) const override;
        virtual void SetDefaults() override;
        virtual void Save(NYson::IYsonConsumer* consumer) const override;
        virtual bool CanOmitValue() const override;
        virtual const std::vector<TString>& GetAliases() const override;
        virtual IMapNodePtr GetUnrecognizedRecursively() const override;
        virtual void SetKeepUnrecognizedRecursively() override;

    public:
        TParameter& Optional();
        TParameter& Default(const T& defaultValue = T());
        TParameter& DefaultNew();
        TParameter& CheckThat(TPostprocessor validator);
        TParameter& GreaterThan(TValueType value);
        TParameter& GreaterThanOrEqual(TValueType value);
        TParameter& LessThan(TValueType value);
        TParameter& LessThanOrEqual(TValueType value);
        TParameter& InRange(TValueType lowerBound, TValueType upperBound);
        TParameter& NonEmpty();
        TParameter& Alias(const TString& name);
        TParameter& MergeBy(EMergeStrategy strategy);

    private:
        T& Parameter;
        TNullable<T> DefaultValue;
        std::vector<TPostprocessor> Postprocessors;
        std::vector<TString> Aliases;
        EMergeStrategy MergeStrategy;
        bool KeepUnrecognizedRecursively = false;
    };

public:
    TYsonSerializableLite();

    void Load(
        NYTree::INodePtr node,
        bool postprocess = true,
        bool setDefaults = true,
        const NYPath::TYPath& path = "");

    void Postprocess(const NYPath::TYPath& path = "") const;

    void SetDefaults();

    void Save(
        NYson::IYsonConsumer* consumer,
        bool stable = false) const;

    IMapNodePtr GetUnrecognized() const;
    IMapNodePtr GetUnrecognizedRecursively() const;

    void SetUnrecognizedStrategy(EUnrecognizedStrategy strategy);

    THashSet<TString> GetRegisteredKeys() const;

protected:
    template <class T>
    TParameter<T>& RegisterParameter(
        const TString& parameterName,
        T& value);

    void RegisterPreprocessor(const TPreprocessor& func);
    void RegisterPostprocessor(const TPostprocessor& func);

private:
    template <class T>
    friend class TParameter;

    THashMap<TString, IParameterPtr> Parameters;

    NYTree::IMapNodePtr Unrecognized;
    EUnrecognizedStrategy UnrecognizedStrategy = EUnrecognizedStrategy::Drop;

    std::vector<TPreprocessor> Preprocessors;
    std::vector<TPostprocessor> Postprocessors;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializable
    : public TRefCounted
    , public TYsonSerializableLite
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> CloneYsonSerializable(TIntrusivePtr<T> obj);

void Serialize(const TYsonSerializableLite& value, NYson::IYsonConsumer* consumer);
void Deserialize(TYsonSerializableLite& value, NYTree::INodePtr node);

NYson::TYsonString ConvertToYsonStringStable(const TYsonSerializableLite& value);

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    TIntrusivePtr<T> obj,
    NYTree::INodePtr patch);

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    TIntrusivePtr<T> obj,
    const NYson::TYsonString& patch);

template <class T>
bool ReconfigureYsonSerializable(
    TIntrusivePtr<T> config,
    const NYson::TYsonString& newConfigYson);

template <class T>
bool ReconfigureYsonSerializable(
    TIntrusivePtr<T> config,
    NYTree::INodePtr newConfigNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TBinaryYsonSerializer
{
    static void Save(TStreamSaveContext& context, const NYTree::TYsonSerializableLite& obj);
    static void Load(TStreamLoadContext& context, NYTree::TYsonSerializableLite& obj);
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, NYTree::TYsonSerializableLite&>>::TType>
{
    typedef TBinaryYsonSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define YSON_SERIALIZABLE_INL_H_
#include "yson_serializable-inl.h"
#undef YSON_SERIALIZABLE_INL_H_
