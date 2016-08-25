#pragma once

#include "common.h"

#include <memory>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An analogue of Java or .NET |object| type.
/*!
 *  Similar to TVariant but more flexible and expensive:
 *  1. Can store values of arbitrary types, no closed list must be known in advance.
 *  2. Supports null semantics, default constructible.
 *  3. Unpacking a value of a given type incurs |dynamic_cast| penalty.
 *  4. Has no tags support.
 */
class TAny
{
public:
    //! Constructs a null instance.
    TAny();

    //! Constructs a null instance.
    TAny(TNull);

    //! Constructs an instance by copying another instance.
    TAny(const TAny& other);

    //! Constructs an instance by moving another instance.
    TAny(TAny&& other);

    //! Constructs an instance by copying a given value.
    template <class T>
    TAny(const T& value);

    //! Constructs an instance by moving a given value.
    template <
        class T,
        class = typename std::enable_if<
            !std::is_reference<T>::value &&
            !std::is_same<typename std::decay<T>::type, TAny>::value
        >::type
    >
    TAny(T&& value);


    //! Assigns a given value.
    template <class T>
    TAny& operator= (const T& value);

    //! Moves a given value.
    template <
        class T,
        class = typename std::enable_if<
            !std::is_reference<T>::value &&
            !std::is_same<typename std::decay<T>::type, TAny>::value
        >::type
    >
    TAny& operator= (T&& value);

    //! Assigns a given instance.
    TAny& operator= (const TAny& other);

    //! Moves a given instance.
    TAny& operator= (TAny&& other);


    //! Checks the instance for null.
    explicit operator bool() const;


    //! Casts the instance to a given type.
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
    struct TUntypedBox;

    template <class T>
    struct TTypedBox;

    std::unique_ptr<TUntypedBox> Box_;


    template <class T>
    void AssignValue(const T& value);

    template <class T>
    void AssignValue(T&& value);

    void AssignAny(const TAny& other);

    void AssignAny(TAny&& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ANY_INL_H_
#include "any-inl.h"
#undef ANY_INL_H_
