#pragma once

#include "serialize.h"
#include "id_generator.h"
#include "mpl.h"

#include <core/actions/callback.h>

#include <typeinfo>

namespace NYT {
namespace NPhoenix {

////////////////////////////////////////////////////////////////////////////////

const ui32 InlineObjectIdMask = 0x80000000;
const ui32 NullObjectId       = 0x00000000;

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTag
{
    virtual ~TDynamicTag()
    { }
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
    typedef T TBase;
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
    typedef TDynamicTag TBase;
};

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedFactory
{
    template <class T>
    static T* Instantiate()
    {
        auto obj = New<T>();
        obj->Ref();
        return obj.Get();
    }
};

template <class T, class = void>
struct TFactoryTraits
{
    typedef TRefCountedFactory TFactory;
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
        NMpl::TIsConvertible<T&, TFactoryTag<TSimpleFactory>&> 
    >::TType
>
{
    typedef TSimpleFactory TFactory;
};

////////////////////////////////////////////////////////////////////////////////

struct TNullFactory
{
    template <class T>
    static T* Instantiate()
    {
        YUNREACHABLE();
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
    typedef TNullFactory TFactory;
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

    DECLARE_SINGLETON_MIXIN(TRegistry, TStaticInstanceMixin);
    DECLARE_SINGLETON_DELETE_AT_EXIT(TRegistry, false);
    DECLARE_SINGLETON_RESET_AT_FORK(TRegistry, false);

private:
    struct TEntry
        : public TIntrinsicRefCounted
    {
        const std::type_info* TypeInfo;
        ui32 Tag;
        TCallback<void*()> Factory;
    };

    typedef TIntrusivePtr<TEntry> TEntryPtr;

    TRegistry();

    ~TRegistry();

    const TEntry& GetEntry(ui32 tag);
    const TEntry& GetEntry(const std::type_info& typeInfo);

    template <class T>
    static void* DoInstantiate();

    yhash_map<const std::type_info*, TEntryPtr> TypeInfoToEntry;
    yhash_map<ui32, TEntryPtr> TagToEntry;
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

#define DEFINE_DYNAMIC_PHOENIX_TYPE(type)                             \
    decltype(type::DynamicPhoenixInitializer)                         \
        type::DynamicPhoenixInitializer

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
    mutable TIdGenerator IdGenerator;

    struct TEntry
    {
        ui32 Id;
        const std::type_info* TypeInfo;
    };

    mutable yhash_map<void*, TEntry> PtrToEntry;

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
    yhash_map<ui32, void*> IdToPtr;

    template <class T, class>
    friend struct TInstantiatedRegistrar;

    std::vector<TIntrinsicRefCounted*> IntrinsicInstantiated;
    std::vector<TExtrinsicRefCounted*> ExtrinsicInstantiated;

};

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct ICustomPersistent
    : public virtual TDynamicTag
{
    virtual void Persist(C& context) = 0;
};

typedef TCustomPersistenceContext<TSaveContext, TLoadContext> TPersistenceContext;
typedef ICustomPersistent<TPersistenceContext> IPersistent;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NPhoenix

#define PHOENIX_INL_H_
#include "phoenix-inl.h"
#undef PHOENIX_INL_H_
