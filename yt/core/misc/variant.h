#pragma once

#include "common.h"

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

    //! Constructs an instance by copying a given value.
    template <class T>
    TVariant(const T& value);

    //! Constructs an instance by moving a given value.
    template <class T>
    TVariant(T&& value);

    //! Destroys the instance.
    ~TVariant();

    //! Assigns a given value.
    template <class T>
    void operator= (const T& value);

    //! Moves a given value.
    template <class T>
    void operator= (T&& value);

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
    std::aligned_union_t<0, Ts...> Storage_;

    template <class T>
    void Assign(const T& value);

    template <class T>
    void Assign(T&& value);

    void Assign(const TVariant& other);

    void Assign(TVariant&& other);

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
