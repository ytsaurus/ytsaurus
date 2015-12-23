#pragma once

#include "common.h"

#include <memory>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
namespace NVariant {

template <class... Ts>
struct TStorageTraits;

template <class X, class... Ts>
struct TTagTraits;

template <class... Ts>
struct TTypeTraits;

} // namespace NVariant
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct TVariantTypeTag {};

//! |boost::variant|-like discriminated union with C++ 11 features.
template <class... Ts>
class TVariant
{
public:
    static_assert(
        ::NYT::NDetail::NVariant::TTypeTraits<Ts...>::NoRefs,
        "TVariant type arguments cannot be references.");

    static_assert(
        ::NYT::NDetail::NVariant::TTypeTraits<Ts...>::NoDuplicates,
        "TVariant type arguments cannot contain duplicate types.");

    //! Variants cannot be default-constructed.
    TVariant() = delete;

    //! Constructs an instance by copying another instance.
    TVariant(const TVariant& other);

    //! Constructs an instance by moving another instance.
    TVariant(TVariant&& other);

    //! Constructs an instance by copying a given value.
    template <class T>
    TVariant(const T& value);

    //! Constructs an instance by moving a given value.
    template <
        class T,
        class = typename std::enable_if<
            !std::is_reference<T>::value &&
            !std::is_same<typename std::decay<T>::type, TVariant<Ts...>>::value
        >::type
    >
    TVariant(T&& value);

    //! Constructs an instance in-place.
    template<
        class T,
        class... TArgs,
        class = typename std::enable_if<
            !std::is_reference<T>::value &&
            !std::is_same<typename std::decay<T>::type, TVariant<Ts...>>::value
        >::type
    >
    TVariant(TVariantTypeTag<T>, TArgs&&... args);

    //! Destroys the instance.
    ~TVariant();

    //! Assigns a given value.
    template <class T>
    TVariant& operator= (const T& value);

    //! Moves a given value.
    template <
        class T,
        class = typename std::enable_if<
            !std::is_reference<T>::value &&
            !std::is_same<typename std::decay<T>::type, TVariant<Ts...>>::value
        >::type
    >
    TVariant& operator= (T&& value);

    //! Assigns a given instance.
    TVariant& operator= (const TVariant& other);

    //! Moves a given instance.
    TVariant& operator= (TVariant&& other);

    //! Returns the discriminating tag of the instance.
    int Tag() const;

    //! Returns the discriminating tag the given type.
    template <class T>
    static constexpr int TagOf()
    {
        // NB: Must keep this inline due to constexpr.
        return ::NYT::NDetail::NVariant::TTagTraits<T, Ts...>::Tag;
    }

    //! Casts the instance to a given type.
    //! Tag validation is only performed in debug builds.
    template <class T>
    T& As();

    //! Similar to its non-const version.
    template <class T>
    const T& As() const;

    //! Checks if the instance holds a given of a given type.
    //! Returns the pointer to the value on success or |nullptr| on failure.
    template <class T>
    T* TryAs();

    //! Similar to its non-const version.
    template <class T>
    const T* TryAs() const;

    //! Returns |true| iff the instance holds a value of a given type.
    template <class T>
    bool Is() const;

private:
    int Tag_;
    typename std::aligned_union<0, Ts...>::type Storage_;

    template <class T>
    void AssignValue(const T& value);

    template <class T>
    void AssignValue(T&& value);

    template <class T, class... TArgs>
    void EmplaceValue(TArgs&&... args);

    void AssignVariant(const TVariant& other);

    void AssignVariant(TVariant&& other);

    void Destroy();

    template <class T>
    T& UncheckedAs();

    template <class T>
    const T& UncheckedAs() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define VARIANT_INL_H_
#include "variant-inl.h"
#undef VARIANT_INL_H_
