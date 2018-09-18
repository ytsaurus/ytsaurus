#pragma once

#include "id_generator.h"
#include "mpl.h"
#include "serialize.h"

#include <yt/core/actions/callback.h>

#include <typeinfo>

namespace NYT {
namespace NPhoenix {

////////////////////////////////////////////////////////////////////////////////

const ui32 InlineObjectIdMask = 0x80000000;
const ui32 NullObjectId       = 0x00000000;

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTag
{
    virtual ~TDynamicTag() = default;
};

template <class TFactory>
struct TFactoryTag
{ };

////////////////////////////////////////////////////////////////////////////////

struct TSerializer;

class TContextBase
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TPolymorphicTraits
{
    static const bool Dynamic = false;
    using TBase = T;
};

template <class T>
struct TPolymorphicTraits<
    T,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T&, TDynamicTag&>
    >::TType
>
{
    static const bool Dynamic = true;
    using TBase = TDynamicTag;
};

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedFactory
{
    template <class T>
    static T* Instantiate()
    {
        return New<T>().Release();
    }
};

template <class T, class = void>
struct TFactoryTraits
{
    using TFactory = TRefCountedFactory;
};

////////////////////////////////////////////////////////////////////////////////

struct TSimpleFactory
{
    template <class T>
    static T* Instantiate()
    {
        return new T();
    }
};

template <class T>
struct TFactoryTraits<
    T,
    typename NMpl::TEnableIf<
        NMpl::TOr<
            NMpl::TIsConvertible<T&, TFactoryTag<TSimpleFactory>&>,
            NMpl::TIsConvertible<T&, ::google::protobuf::Message&>
        >
    >::TType
>
{
    using TFactory = TSimpleFactory;
};

////////////////////////////////////////////////////////////////////////////////

struct TNullFactory
{
    template <class T>
    static T* Instantiate()
    {
        Y_UNREACHABLE();
    }
};

template <class T>
struct TFactoryTraits<
    T,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T&, TFactoryTag<TNullFactory>&>
    >::TType
>
{
    using TFactory = TNullFactory;
};

////////////////////////////////////////////////////////////////////////////////

class TRegistry
{
public:
    static TRegistry* Get();

    ui32 GetTag(const std::type_info& typeInfo);

    template <class T>
    T* Instantiate(ui32 tag);

    template <class T>
    void Register(ui32 tag);

private:
    struct TEntry
    {
        const std::type_info* TypeInfo;
        ui32 Tag;
        std::function<void*()> Factory;
    };

    THashMap<const std::type_info*, TEntry*> TypeInfoToEntry_;
    THashMap<ui32, TEntry> TagToEntry_;

    TRegistry();

    const TEntry& GetEntry(ui32 tag);
    const TEntry& GetEntry(const std::type_info& typeInfo);

    template <class T>
    static void* DoInstantiate();

    Y_DECLARE_SINGLETON_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

template <
    class TType,
    ui32 tag,
    class TFactory = TRefCountedFactory
>
struct TDynamicInitializer
{
    TDynamicInitializer()
    {
        TRegistry::Get()->Register<TType>(tag);
    }
};

#define DECLARE_DYNAMIC_PHOENIX_TYPE(...)                             \
    static ::NYT::NPhoenix::TDynamicInitializer<__VA_ARGS__>          \
        DynamicPhoenixInitializer

// __VA_ARGS__ are used because sometimes we want a template type
// to be an argument but the single macro argument may not contain
// commas. Dat preprocessor :/
#define DEFINE_DYNAMIC_PHOENIX_TYPE(...)                              \
    decltype(__VA_ARGS__::DynamicPhoenixInitializer)                  \
        __VA_ARGS__::DynamicPhoenixInitializer

#define INHERIT_DYNAMIC_PHOENIX_TYPE(baseType, type, tag)             \
class type                                                            \
    : public baseType                                                 \
{                                                                     \
public:                                                               \
    using baseType::baseType;                                         \
                                                                      \
private:                                                              \
    DECLARE_DYNAMIC_PHOENIX_TYPE(type, tag);                          \
};

#define INHERIT_DYNAMIC_PHOENIX_TYPE_TEMPLATED(baseType, type, tag, ...) \
class type                                                               \
    : public baseType<__VA_ARGS__>                                       \
{                                                                        \
public:                                                                  \
    using baseType::baseType;                                            \
                                                                         \
private:                                                                 \
    DECLARE_DYNAMIC_PHOENIX_TYPE(type, tag);                             \
};

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public TContextBase
    , public TStreamSaveContext
{
public:
    TSaveContext();

    ui32 FindId(void* basePtr, const std::type_info* typeInfo) const;
    ui32 GenerateId(void* basePtr, const std::type_info* typeInfo);

private:
    mutable TIdGenerator IdGenerator_;

    struct TEntry
    {
        ui32 Id;
        const std::type_info* TypeInfo;
    };

    mutable THashMap<void*, TEntry> PtrToEntry_;

};

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TInstantiatedRegistrar;

class TLoadContext
    : public TContextBase
    , public TStreamLoadContext
{
public:
    ~TLoadContext();

    void RegisterObject(ui32 id, void* basePtr);
    void* GetObject(ui32 id) const;

    template <class T>
    void RegisterInstantiatedObject(T* rawPtr);

private:
    THashMap<ui32, void*> IdToPtr_;

    template <class T, class>
    friend struct TInstantiatedRegistrar;

    std::vector<std::function<void()>> Deletors_;

};

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct ICustomPersistent
    : public virtual TDynamicTag
{
    virtual ~ICustomPersistent() = default;
    virtual void Persist(const C& context) = 0;
};

using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext>;
using IPersistent = ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPhoenix
} // namespace NYT

#define PHOENIX_INL_H_
#include "phoenix-inl.h"
#undef PHOENIX_INL_H_
