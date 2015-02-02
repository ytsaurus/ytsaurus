#ifndef VARIANT_INL_H_
#error "Direct inclusion of this file is not allowed, include variant.h"
#endif
#undef VARIANT_INL_H_

#include <type_traits>

#include <core/misc/mpl.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
namespace NVariant {

template <class T, class... Ts>
struct TStorageTraits<T, Ts...>
{
    static void Destroy(int tag, void* storage)
    {
        if (tag == 0) {
            reinterpret_cast<T*>(storage)->~T();
        } else {
            TStorageTraits<Ts...>::Destroy(tag - 1, storage);
        }
    }

    template <class V>
    static void CopyConstruct(int tag, void* storage, const V& other)
    {
        if (tag == 0) {
            new (storage) T(other.template As<T>());
        } else {
            TStorageTraits<Ts...>::CopyConstruct(tag - 1, storage, other);
        }
    }

    template <class V>
    static void MoveConstruct(int tag, void* storage, V&& other)
    {
        if (tag == 0) {
            new (storage) T(static_cast<T&&>(other.template As<T>()));
        } else {
            TStorageTraits<Ts...>::MoveConstruct(tag - 1, storage, other);
        }
    }

};

template <>
struct TStorageTraits<>
{
    static void Destroy(int /*tag*/, void* /*storage*/)
    {
        // Invalid TVariant tag.
        YUNREACHABLE();
    }

    template <class V>
    static void CopyConstruct(int /*tag*/, void* /*storage*/, const V& /*other*/)
    {
        // Invalid TVariant tag.
        YUNREACHABLE();
    }

    template <class V>
    static void MoveConstruct(int /*tag*/, void* /*storage*/, V&& /*other*/)
    {
        // Invalid TVariant tag.
        YUNREACHABLE();
    }
};

template <class X>
struct TTagTraits<X>
{
    static const int Tag = -1;
};

template <class X, class... Ts>
struct TTagTraits<X, X, Ts...>
{
    static const int Tag = 0;
};

template <class X, class T, class... Ts>
struct TTagTraits<X, T, Ts...>
{
    static const int Tag = TTagTraits<X, Ts...>::Tag != -1 ? TTagTraits<X, Ts...>::Tag + 1 : -1;
};

template <class T, class... Ts>
struct TTypeTraits<T, Ts...>
{
    static const bool NoRefs = NMpl::TIsReference<T>::Value || TTypeTraits<Ts...>::NoRefs;
    static const bool NoDuplicates = TTagTraits<T, Ts...>::Tag == -1 && TTypeTraits<Ts...>::NoDuplicates;
};

template <>
struct TTypeTraits<>
{
    static const bool NoRefs = true;
    static const bool NoDuplicates = true;
};

} // namespace NVariant
} // namespace NDetail

template <class... Ts>
template <class T>
void TVariant<Ts...>::AssignValue(const T& value)
{
    static_assert(
        ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag != -1,
        "Type not in TVariant.");

    Tag_ = ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag;
    new (&Storage_) T(value);
}

template <class... Ts>
template <class T>
void TVariant<Ts...>::AssignValue(T&& value)
{
    static_assert(
        ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag != -1,
        "Type not in TVariant.");

    Tag_ = ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag;
    new (&Storage_) T(std::move(value));
}

template <class... Ts>
template <class T, class... TArgs>
void TVariant<Ts...>::EmplaceValue(TArgs&&... args)
{
    static_assert(
        ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag != -1,
        "Type not in TVariant.");

    Tag_ = ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag;
    new (&Storage_) T(std::forward<TArgs>(args)...);
}

template <class... Ts>
void TVariant<Ts...>::AssignVariant(const TVariant& other)
{
    Tag_ = other.Tag_;
    ::NYT::NDetail::NVariant::TStorageTraits<Ts...>::CopyConstruct(Tag_, &Storage_, other);
}

template <class... Ts>
void TVariant<Ts...>::AssignVariant(TVariant&& other)
{
    Tag_ = other.Tag_;
    ::NYT::NDetail::NVariant::TStorageTraits<Ts...>::MoveConstruct(Tag_, &Storage_, other);
}

template <class... Ts>
template <class T>
T& TVariant<Ts...>::UncheckedAs()
{
    return *reinterpret_cast<T*>(&Storage_);
}

template <class... Ts>
template <class T>
const T& TVariant<Ts...>::UncheckedAs() const
{
    return *reinterpret_cast<const T*>(&Storage_);
}

template <class... Ts>
template <class T>
TVariant<Ts...>::TVariant(const T& value)
{
    AssignValue(value);
}

template <class... Ts>
template <class T, class>
TVariant<Ts...>::TVariant(T&& value)
{
    AssignValue(std::move(value));
}

template <class... Ts>
template<class T, class... TArgs, class>
TVariant<Ts...>::TVariant(TVariantTypeTag<T>, TArgs&&... args)
{
    EmplaceValue<T>(std::forward<TArgs>(args)...);
}

template <class... Ts>
TVariant<Ts...>::TVariant(const TVariant& other)
{
    AssignVariant(other);
}

template <class... Ts>
TVariant<Ts...>::TVariant(TVariant&& other)
{
    AssignVariant(std::move(other));
}

template <class... Ts>
template <class T>
TVariant<Ts...>& TVariant<Ts...>::operator=(const T& value)
{
    if (&value != &UncheckedAs<T>()) {
        Destroy();
        AssignValue(value);
    }
    return *this;
}

template <class... Ts>
template <class T, class>
TVariant<Ts...>& TVariant<Ts...>::operator=(T&& value)
{
    if (&value != &UncheckedAs<T>()) {
        Destroy();
        AssignValue(std::move(value));
    }
    return *this;
}

template <class... Ts>
TVariant<Ts...>& TVariant<Ts...>::operator=(const TVariant& other)
{
    if (&other != this) {
        Destroy();
        AssignVariant(other);
    }
    return *this;
}

template <class... Ts>
TVariant<Ts...>& TVariant<Ts...>::operator=(TVariant&& other)
{
    if (&other != this) {
        Destroy();
        AssignVariant(std::move(other));
    }
    return *this;
}

template <class... Ts>
template <class T>
T& TVariant<Ts...>::As()
{
    typedef ::NYT::NDetail::NVariant::TTagTraits<T, Ts...> TTagTraits;
    static_assert(TTagTraits::Tag != -1, "Type not in TVariant.");
    YASSERT(Tag_ == TTagTraits::Tag);
    return UncheckedAs<T>();
}

template <class... Ts>
template <class T>
const T& TVariant<Ts...>::As() const
{
    typedef ::NYT::NDetail::NVariant::TTagTraits<T, Ts...> TTagTraits;
    static_assert(TTagTraits::Tag != -1, "Type not in TVariant.");
    YASSERT(Tag_ == TTagTraits::Tag);
    return UncheckedAs<T>();
}

template <class... Ts>
template <class T>
T* TVariant<Ts...>::TryAs()
{
    typedef ::NYT::NDetail::NVariant::TTagTraits<T, Ts...> TTagTraits;
    static_assert(TTagTraits::Tag != -1, "Type not in TVariant.");
    return Tag_ == TTagTraits::Tag ? &UncheckedAs<T>() : nullptr;
}

template <class... Ts>
template <class T>
const T* TVariant<Ts...>::TryAs() const
{
    typedef ::NYT::NDetail::NVariant::TTagTraits<T, Ts...> TTagTraits;
    static_assert(TTagTraits::Tag != -1, "Type not in TVariant.");
    return Tag_ == TTagTraits::Tag ? &UncheckedAs<T>() : nullptr;
}

template <class... Ts>
template <class T>
bool TVariant<Ts...>::Is() const
{
    typedef ::NYT::NDetail::NVariant::TTagTraits<T, Ts...> TTagTraits;
    static_assert(TTagTraits::Tag != -1, "Type not in TVariant.");
    return Tag_ == TTagTraits::Tag;
}

template <class... Ts>
TVariant<Ts...>::~TVariant()
{
    Destroy();
}

template <class... Ts>
void TVariant<Ts...>::Destroy()
{
    ::NYT::NDetail::NVariant::TStorageTraits<Ts...>::Destroy(Tag_, &Storage_);
}

template <class... Ts>
int TVariant<Ts...>::Tag() const
{
    return Tag_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
