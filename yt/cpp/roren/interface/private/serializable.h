#pragma once

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/serialize.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/ptr.h>
#include <util/ysaveload.h>

#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class IClonable
    : public virtual TThrRefBase
{
public:
    virtual ::TIntrusivePtr<TDerived> Clone() const = 0;
};

template <typename TDerived>
class ISerializable
    : public IClonable<TDerived>
{
public:
    using TDefaultFactoryFunc = ::TIntrusivePtr<TDerived> (*)();

    ::TIntrusivePtr<TDerived> Clone() const override;

private:
    virtual TDefaultFactoryFunc GetDefaultFactory() const = 0;
    virtual void Save(IOutputStream* stream) const = 0;
    virtual void Load(IInputStream* stream) = 0;

private:
    template <typename T>
    friend NYT::TNode SerializableToNode(const ISerializable<T>&);

    template <typename T>
    friend ::TIntrusivePtr<T> SerializableFromNode(const NYT::TNode& description);
};


template <typename TDerived>
::TIntrusivePtr<TDerived> ISerializable<TDerived>::Clone() const
{
    TString state;

    {
        TStringOutput out(state);
        Save(&out);
        out.Finish();
    }

    auto newObject = GetDefaultFactory()();
    {
        TStringInput in(state);
        newObject->Load(&in);
    }
    return newObject;
}

template <typename T>
NYT::TNode SerializableToNode(const ISerializable<T>& serializable)
{
    TStringStream state;
    serializable.Save(&state);
    NYT::TNode result;
    result["default_factory"] = reinterpret_cast<ui64>(serializable.GetDefaultFactory());
    result["state"] = state.Str();
    return result;
}

template <typename T>
::TIntrusivePtr<T> SerializableFromNode(const NYT::TNode& description)
{
    static_assert(std::is_base_of_v<ISerializable<T>, T>);

    auto defaultFactory = reinterpret_cast<typename ISerializable<T>::TDefaultFactoryFunc>(
        description["default_factory"].AsUint64()
    );
    auto computation = defaultFactory();
    auto state = description["state"].AsString();
    TStringInput stringInput(state);
    computation->Load(&stringInput);
    return computation;
}

template <class T>
static inline void SaveSerializable(IOutputStream* out, const ::TIntrusivePtr<T>& t) {
    static_assert(std::is_base_of_v<ISerializable<T>, T>);
    auto node = t ? SerializableToNode(*t) : NYT::TNode::CreateEntity();
    auto s = NodeToYsonString(node, NYson::EYsonFormat::Binary);
    ::Save(out, s);
}

template <class T>
static inline void LoadSerializable(IInputStream* in, ::TIntrusivePtr<T>& t) {
    static_assert(std::is_base_of_v<ISerializable<T>, T>);
    TString s;
    ::Load(in, s);
    auto node = NYT::NodeFromYsonString(s);
    if (node.IsNull()) {
        t = nullptr;
    } else {
        t = SerializableFromNode<T>(node);
    }
}

template <typename T>
void SaveThroughYson(IOutputStream* output, const T& value)
{
    TStringStream yson;
    {
        NYson::TYsonWriter writer(&yson);
        Serialize(value, &writer);
    }
    ::Save(output, yson.Str());
}

template <typename T>
void LoadThroughYson(IInputStream* input, T& value)
{
    TString yson;
    ::Load(input, yson);
    auto node = NYT::NodeFromYsonString(yson);

    Deserialize(value, node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
