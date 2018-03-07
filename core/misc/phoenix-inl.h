#pragma once
#ifndef PHOENIX_INL_H_
#error "Direct inclusion of this file is not allowed, include phoenix.h"
#endif

namespace NYT {
namespace NPhoenix {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void* TRegistry::DoInstantiate()
{
    using TFactory = typename TFactoryTraits<T>::TFactory;
    using TBase = typename TPolymorphicTraits<T>::TBase;

    T* ptr = TFactory::template Instantiate<T>();
    TBase* basePtr = static_cast<TBase*>(ptr);
    return basePtr;
}

template <class T>
void TRegistry::Register(ui32 tag)
{
    auto pair = TagToEntry_.emplace(tag, TEntry());
    YCHECK(pair.second);
    auto& entry = pair.first->second;
    entry.Tag = tag;
    entry.TypeInfo = &typeid (T);
    entry.Factory = std::bind(&DoInstantiate<T>);
    YCHECK(TypeInfoToEntry_.insert(std::make_pair(entry.TypeInfo, &entry)).second);
}

template <class T>
T* TRegistry::Instantiate(ui32 tag)
{
    using TBase = typename TPolymorphicTraits<T>::TBase;
    TBase* basePtr = static_cast<TBase*>(GetEntry(tag).Factory());
    return dynamic_cast<T*>(basePtr);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class>
struct TInstantiatedRegistrar
{
    static void Do(TLoadContext& /*context*/, T* /*rawPtr*/)
    { }
};

template <class T>
struct TInstantiatedRegistrar<
    T,
    typename NMpl::TEnableIfC<
        NMpl::TIsConvertible<T&, TRefCountedImpl<false>&>::Value ||
        NMpl::TIsConvertible<T&, TRefCountedImpl<true>&>::Value
    >::TType
>
{
    static void Do(TLoadContext& context, T* rawPtr)
    {
        context.Deletors_.push_back([=] { Unref(rawPtr); });
    }
};

template <class T>
void TLoadContext::RegisterInstantiatedObject(T* rawPtr)
{
    TInstantiatedRegistrar<T>::Do(*this, rawPtr);
}

////////////////////////////////////////////////////////////////////////////////

struct TSerializer
{
    template <class T, class C>
    static void Save(C& context, const TIntrusivePtr<T>& ptr)
    {
        SaveImpl(context, ptr.Get());
    }

    template <class T, class C>
    static void Save(C& context, const std::unique_ptr<T>& ptr)
    {
        SaveImpl(context, ptr.get());
    }

    template <class T, class C>
    static void Save(C& context, T* ptr)
    {
        SaveImpl(context, ptr);
    }

    template <class T, class C>
    static void SaveImpl(C& context, T* ptr)
    {
        using TBase = typename TPolymorphicTraits<T>::TBase;
        using NYT::Save;

        if (ptr) {
            TBase* basePtr = ptr;
            bool dynamic = TPolymorphicTraits<T>::Dynamic;
            const auto* typeInfo = dynamic ? &typeid(*ptr) : nullptr;
            bool saveBody = false;
            ui32 id = context.FindId(basePtr, typeInfo);
            if (!id) {
                id = context.GenerateId(basePtr, typeInfo);
                saveBody = true;
            }

            Save(context, saveBody ? (id | InlineObjectIdMask) : id);

            if (saveBody) {
                if (dynamic) {
                    ui32 tag = TRegistry::Get()->GetTag(*typeInfo);
                    Save(context, tag);
                }
                Save(context, *ptr);
            }
        } else {
            Save(context, NullObjectId);
        }
    }


    template <class T, class C>
    static void Load(C& context, TIntrusivePtr<T>& ptr)
    {
        T* rawPtr = nullptr;
        LoadImpl(context, rawPtr, false);
        ptr.Reset(rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, const TIntrusivePtr<T>& ptr)
    {
        T* rawPtr = ptr.Get();
        LoadImpl(context, rawPtr, true);
    }

    template <class T, class C>
    static void Load(C& context, std::unique_ptr<T>& ptr)
    {
        T* rawPtr = nullptr;
        LoadImpl(context, rawPtr, false);
        ptr.reset(rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, const std::unique_ptr<T>& ptr)
    {
        T* rawPtr = ptr.get();
        LoadImpl(context, rawPtr, true);
    }

    template <class T, class C>
    static void Load(C& context, T*& rawPtr)
    {
        rawPtr = nullptr;
        LoadImpl(context, rawPtr, false);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, T* rawPtr)
    {
        LoadImpl(context, rawPtr, true);
    }

    template <class T, class C>
    static void LoadImpl(C& context, T*& rawPtr, bool inplace)
    {
        using TBase = typename TPolymorphicTraits<T>::TBase;
        using NYT::Load;

        ui32 id = Load<ui32>(context);
        if (id & InlineObjectIdMask) {
            if (inplace) {
                YCHECK(rawPtr);
                TInstantiator<T, C, TPolymorphicTraits<T>::Dynamic>::ValidateTag(context, rawPtr);
            } else {
                rawPtr = TInstantiator<T, C, TPolymorphicTraits<T>::Dynamic>::Instantiate(context);
                context.RegisterInstantiatedObject(rawPtr);
            }

            TBase* basePtr = rawPtr;
            context.RegisterObject(id & ~InlineObjectIdMask, basePtr);

            Load(context, *rawPtr);
        } else {
            if (id) {
                auto* basePtr = static_cast<TBase*>(context.GetObject(id));
                rawPtr = dynamic_cast<T*>(basePtr);
            } else {
                rawPtr = nullptr;
            }
        }
    }


    template <class T, class C, bool dynamic>
    struct TInstantiator
    { };

    template <class T, class C>
    struct TInstantiator<T, C, true>
    {
        static T* Instantiate(C& context)
        {
            using NYT::Load;
            ui32 tag = Load<ui32>(context);
            return TRegistry::Get()->Instantiate<T>(tag);
        }

        static void ValidateTag(C& context, T* rawPtr)
        {
            using NYT::Load;
            ui32 streamTag = Load<ui32>(context);
            ui32 runtimeTag = TRegistry::Get()->GetTag(typeid (*rawPtr));
            YCHECK(streamTag == runtimeTag);
        }
    };

    template <class T, class C>
    struct TInstantiator<T, C, false>
    {
        static T* Instantiate(const C& /*context*/)
        {
            using TFactory = typename TFactoryTraits<T>::TFactory;
            return TFactory::template Instantiate<T>();
        }

        static void ValidateTag(const C& /*context*/, T* /*rawPtr*/)
        { }
    };

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPhoenix
} // namespace NYT

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIfC<
        NMpl::TAndC<
            NMpl::TIsConvertible<T&, TIntrusivePtr<typename T::TUnderlying>&>::Value,
            NMpl::TIsConvertible<C&, NPhoenix::TContextBase&>::Value
        >::Value
    >::TType
>
{
    using TSerializer = NPhoenix::TSerializer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIfC<
        NMpl::TAndC<
            NMpl::TIsConvertible<T&, std::unique_ptr<typename T::element_type>&>::Value,
            NMpl::TIsConvertible<C&, NPhoenix::TContextBase&>::Value
        >::Value
    >::TType
>
{
    using TSerializer = NPhoenix::TSerializer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIfC<
        NMpl::TAndC<
            std::is_pointer<T>::value,
            NMpl::TIsConvertible<C&, NPhoenix::TContextBase&>::Value
        >::Value
    >::TType
>
{
    using TSerializer = NPhoenix::TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

