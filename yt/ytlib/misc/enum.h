#pragma once

/*!
 * \file enum.h
 * \brief Smart enumerations
 */

#include "preprocessor.h"
#include "rvalue.h"

#include <util/string/cast.h>
#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * \defgroup yt_enum Smart enumerations
 * \ingroup yt_commons
 *
 * \{
 *
 * \page yt_enum_examples Examples
 * Please refer to the unit test for an actual example of usage
 * (unittests/enum_ut.cpp).
 *
 */

//! Base class for strongly-typed enumerations.
template<class TDerived>
class TEnumBase
{
public:
    //! Default constructor.
    TEnumBase()
        : Value(0)
    { }

    TEnumBase(const TEnumBase<TDerived>& other)
        : Value(other.Value)
    { }

    TEnumBase(TEnumBase<TDerived>&& other)
        : Value(other.Value)
    { }

    //! Explicit constructor.
    explicit TEnumBase(int value)
        : Value(value)
    { }

    //! Returns underlying integral value.
    int ToValue() const
    {
        return Value;
    }

protected:
    int Value;
};

//! Base class for polymorphic enumerations.
template<class TDerived>
class TPolymorphicEnumBase
{
public:
    //! Default constructor.
    TPolymorphicEnumBase()
        : Value(0)
        , Name(NULL)
    { }

    TPolymorphicEnumBase(const TPolymorphicEnumBase<TDerived>& other)
        : Value(other.Value)
        , Name(other.Name)
    { }

    TPolymorphicEnumBase(TPolymorphicEnumBase<TDerived>&& other)
        : Value(other.Value)
        , Name(other.Name)
    { }

    //! Explicit constructor.
    explicit TPolymorphicEnumBase(int value, const char* asString = NULL)
        : Value(value)
        , Name(asString)
    { }

    //! Returns underlying integral value.
    int ToValue() const
    {
        return Value;
    }

protected:
    int Value;
    const char* Name;
};

/*! \} */

////////////////////////////////////////////////////////////////////////////////

//! \internal
//! \defgroup yt_enum_mixins Mix-ins for internals of enumerations.
//! \{

//! Base mix-in for strongly-typed enumeration.
/*!
 * This mix-in declares the following:
 *   - Enumeration domain,
 *   - Default constructor,
 *   - Constructor from a domain value,
 *   - Explicit constructor from an integral value
 *   - Implicit conversion operator to the enumeration domain
 */
#define MIXIN_ENUM__BASE(name, seq) \
    private: \
        friend class ::NYT::TEnumBase<name>; \
    public: \
        enum EDomain \
        { \
            PP_FOR_EACH(DECLARE_ENUM__DOMAIN_ITEM, seq) \
        }; \
        \
        name() \
            : ::NYT::TEnumBase<name>() \
        { } \
        \
        name(const EDomain& e) \
            : ::NYT::TEnumBase<name>(static_cast<int>(e)) \
        { } \
        \
        explicit name(int value) \
            : ::NYT::TEnumBase<name>(value) \
        { } \
        \
        operator EDomain() const \
        { \
            return static_cast<EDomain>(Value); \
        } \
        \
        name& operator=(const name::EDomain& e) \
        { \
            Value = static_cast<int>(e); \
            return *this; \
        }

//! \cond Implementation
//! EDomain declaration helper.
//! \{
#define DECLARE_ENUM__DOMAIN_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        DECLARE_ENUM__DOMAIN_ITEM_SEQ, \
        DECLARE_ENUM__DOMAIN_ITEM_ATOMIC \
    )(item)()

#define DECLARE_ENUM__DOMAIN_ITEM_ATOMIC(item) \
    item PP_COMMA

#define DECLARE_ENUM__DOMAIN_ITEM_SEQ(seq) \
    PP_ELEMENT(seq, 0) = PP_ELEMENT(seq, 1) PP_COMMA
//! \}
//! \endcond

//! ToString() mix-in.
//! \{
#define MIXIN_ENUM__TO_STRING(name, seq) \
    public: \
        Stroka ToString() const \
        { \
            switch (Value) \
            { \
                PP_FOR_EACH(DECLARE_ENUM__TO_STRING_ITEM, seq) \
                default: \
                    return Stroka::Join( \
                        PP_STRINGIZE(name) "(", ::ToString(Value), ")" \
                    ); \
            } \
        }
//! \}

//! \cond Implementation
//! ToString() declaration helper.
//! \{
#define DECLARE_ENUM__TO_STRING_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        DECLARE_ENUM__TO_STRING_ITEM_SEQ, \
        DECLARE_ENUM__TO_STRING_ITEM_ATOMIC \
    )(item)

#define DECLARE_ENUM__TO_STRING_ITEM_SEQ(seq) \
    DECLARE_ENUM__TO_STRING_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define DECLARE_ENUM__TO_STRING_ITEM_ATOMIC(item) \
    case static_cast<int>(item): \
        return PP_STRINGIZE(item);
//! \}
//! \endcond

//! FromString() mix-in.
//! \{
#define MIXIN_ENUM__FROM_STRING(name, seq) \
    public: \
        static name FromString(const Stroka& str) \
        { \
            name target; \
            if (!FromString(str, &target)) { \
                ythrow yexception() \
                    << "Error parsing " PP_STRINGIZE(name) " value '" << str << "'"; \
            } \
            return MoveRV(target); \
        } \
        \
        static bool FromString(const Stroka& str, name* target) \
        { \
            PP_FOR_EACH(DECLARE_ENUM__FROM_STRING_ITEM, seq) \
            return false; \
        }
//! \}

//! \cond Implementation
//! FromString() declaration helper.
//! \{
#define DECLARE_ENUM__FROM_STRING_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        DECLARE_ENUM__FROM_STRING_ITEM_SEQ, \
        DECLARE_ENUM__FROM_STRING_ITEM_ATOMIC \
    )(item)

#define DECLARE_ENUM__FROM_STRING_ITEM_SEQ(seq) \
    DECLARE_ENUM__FROM_STRING_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define DECLARE_ENUM__FROM_STRING_ITEM_ATOMIC(item) \
    if (str == PP_STRINGIZE(item)) { \
        *target = item; \
        return true; \
    }
//! \}
//! \endcond

//! Base mix-in for polymorphic enumeration.
/*!
 * This mix-in declares the following:
 *   - Enumeration domain,
 *   - Default constructor,
 *   - Constructor from a domain value,
 *   - Constructor from a polymorphic enumeration in the same scope
 *   - Explicit constructor from an integral value
 *   - Implicit conversion operator to the enumeration domain
 */
#define MIXIN_POLY_ENUM__BASE(basename, name, seq) \
    private: \
        friend class ::NYT::TPolymorphicEnumBase<basename>; \
    public: \
        enum EDomain \
        { \
            PP_FOR_EACH(DECLARE_ENUM__DOMAIN_ITEM, seq) \
        }; \
        \
        name() \
            : ::NYT::TPolymorphicEnumBase<basename>() \
        { } \
        \
        name(const EDomain& e) \
            : ::NYT::TPolymorphicEnumBase<basename>( \
                static_cast<int>(e), \
                name::TryConvertDomainToString(static_cast<int>(e))) \
        { } \
        \
        name(const ::NYT::TPolymorphicEnumBase<basename>& other) \
            : ::NYT::TPolymorphicEnumBase<basename>(other) \
        { } \
        name(::NYT::TPolymorphicEnumBase<basename>&& other) \
            : ::NYT::TPolymorphicEnumBase<basename>(MoveRV(other)) \
        { } \
        \
        explicit name(int value, const char* asString = NULL) \
            : ::NYT::TPolymorphicEnumBase<basename>( \
                value, asString ? asString : name::TryConvertDomainToString(value) \
            ) \
        { } \
        \
        operator EDomain() const \
        { \
            return static_cast<EDomain>(Value); \
        } \
        \
        name& operator=(const name::EDomain& e) \
        { \
            Value = static_cast<int>(e); \
            Name = name::TryConvertDomainToString(static_cast<int>(e)); \
            return *this; \
        }

//! ToString() mix-in.
#define MIXIN_POLY_ENUM__TO_STRING(basename, name, seq) \
    public: \
        Stroka ToString() const \
        { \
            if (Name) { \
                return Name; \
            } \
            \
            const char* asString = name::TryConvertDomainToString(Value); \
            if (asString) { \
                return asString; \
            } \
            \
            return Stroka::Join(PP_STRINGIZE(name) "(", ::ToString(Value), ")"); \
        }

//! FromString() mix-in.
//! Fallbacks to strongly-typed implementation.
#define MIXIN_POLY_ENUM__FROM_STRING(basename, name, seq) \
    MIXIN_ENUM__FROM_STRING(name, seq)

//! TryConvertDomainToString() mix-in.
//! Fallbacks to chunks of strongly-typed implementation.
#define MIXIN_POLY_ENUM__TRY_CONVERT_DOMAIN_TO_STRING(basename, name, seq) \
    private: \
        static const char* TryConvertDomainToString(int value) { \
            switch (value) \
            { \
                PP_FOR_EACH(DECLARE_ENUM__TO_STRING_ITEM, seq) \
                default: \
                    return NULL; \
            } \
        }
//! \}

//! \endinternal
//! \}

////////////////////////////////////////////////////////////////////////////////

//! Declares a strongly-typed enumeration with explicit integral conversion.
/*!
 * \param name Name of the enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define DECLARE_ENUM(name, seq) \
    BEGIN_DECLARE_ENUM(name, seq) \
    END_DECLARE_ENUM()

//! Begins the declaration of strongly-typed enumeration.
//! See #DECLARE_ENUM.
#define BEGIN_DECLARE_ENUM(name, seq) \
    class name : public ::NYT::TEnumBase<name> \
    { \
        MIXIN_ENUM__BASE(name, seq) \
        MIXIN_ENUM__TO_STRING(name, seq) \
        MIXIN_ENUM__FROM_STRING(name, seq)

//! Ends the declaration of strongly-typed enumeration.
//! See #DECLARE_ENUM.
#define END_DECLARE_ENUM() \
    }

//! Declares a polymorphic enumeration with its own scope.
/*!
 * \param name Name of the enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */

#define DECLARE_POLY_ENUM1(name, seq) \
    BEGIN_DECLARE_POLY_ENUM(name, name, seq) \
    END_DECLARE_POLY_ENUM()

//! Declares a polymorphic enumeration with specified scope.
/*!
 * \param basename Basic enumeration (scope of polymorphism).
 * \param name Name of the enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define DECLARE_POLY_ENUM2(basename, name, seq) \
    BEGIN_DECLARE_POLY_ENUM(basename, name, seq) \
    END_DECLARE_POLY_ENUM()

//! Begins the declaration of polymorphic enumeration.
//! See #DECLARE_POLY_ENUM.
#define BEGIN_DECLARE_POLY_ENUM(basename, name, seq) \
    class name : public ::NYT::TPolymorphicEnumBase<basename> \
    { \
        MIXIN_POLY_ENUM__BASE(basename, name, seq) \
        MIXIN_POLY_ENUM__TO_STRING(basename, name, seq) \
        MIXIN_POLY_ENUM__FROM_STRING(basename, name, seq) \
        MIXIN_POLY_ENUM__TRY_CONVERT_DOMAIN_TO_STRING(basename, name, seq)

//! Ends the declaration of polymorphic enumeration.
//! See #DECLARE_POLY_ENUM.
#define END_DECLARE_POLY_ENUM() \
    }

/*! \} */

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

