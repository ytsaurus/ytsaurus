#pragma once

/*!
 * \file enum.h
 * \brief Smart enumerations
 */

#include "preprocessor.h"
#include "rvalue.h"

#include <util/string/cast.h>
#include <util/generic/typehelpers.h>
#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * \defgroup yt_enum Smart enumerations
 * \ingroup yt_commons
 *
 * \{
 *
 * A string literal could be associated with an instance of polymorphic
 * enumeration and this literal is preserved during casts. Note that
 * the instance does not take ownership of the associated string literal
 * (i. e. does not free() the string in destructor). This is due to the fact
 * that only statically allocated literals are associated with the instance.
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
        : Value(MoveRV(other.Value))
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
        , Literal(NULL)
    { }

    TPolymorphicEnumBase(const TPolymorphicEnumBase<TDerived>& other)
        : Value(other.Value)
        , Literal(other.Literal)
    { }

    TPolymorphicEnumBase(TPolymorphicEnumBase<TDerived>&& other)
        : Value(MoveRV(other.Value))
        , Literal(MoveRV(other.Literal))
    { }

    //! Explicit constructor.
    explicit TPolymorphicEnumBase(int value, const char* literal = NULL)
        : Value(value)
        , Literal(literal)
    { }

    //! Returns underlying integral value.
    int ToValue() const
    {
        return Value;
    }

    //! Checks whether there is an associated string literal with this value.
    bool HasAssociatedStringLiteral() const
    {
        return Literal != NULL;
    }

protected:
    int Value;
    const char* Literal;
};

/*! \} */

////////////////////////////////////////////////////////////////////////////////

//! \internal
//! \defgroup yt_enum_mixins Mix-ins for the internals of enumerations.
//! \{

//! Declaration of an enumeration class.
/*!
 * \param name Name of the enumeration.
 * \param base Base class; either ##TEnumBase<T> or ##TPolymorphicEnumBase<T>.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define ENUM__CLASS(name, base, seq) \
    class name \
        : public base \
    { \
    private: \
        friend class base; \
        typedef base TBase; \
        \
    public: \
        enum EDomain \
        { \
            PP_FOR_EACH(ENUM__DOMAIN_ITEM, seq) \
        }; \
        \
        name() \
            : TBase() \
        { } \
        \
        name(const name& other) \
            : TBase(MoveRV(static_cast<TBase>(other))) \
        { } \
        \
        name(name&& other) \
            : TBase(MoveRV(other)) \
        { } \
        \
        name(const TBase& other) \
            : TBase(other) \
        { } \
        \
        name(TBase&& other) \
            : TBase(MoveRV(other)) \
        { } \
        \
        name(const EDomain& e) \
            : TBase(MoveRV(name::SpawnFauxBase(static_cast<int>(e)))) \
        { } \
        \
        name& operator=(const TBase& other) \
        { \
            TBase::operator=(MoveRV(static_cast<TBase>(other))); \
            return *this; \
        } \
        \
        name& operator=(TBase&& other) \
        { \
            TBase::operator=(MoveRV(other)); \
            return *this; \
        } \
        \
        name& operator=(const EDomain& e) \
        { \
            TBase::operator=(MoveRV(name::SpawnFauxBase(static_cast<int>(e)))); \
            return *this; \
        } \
        \
        operator EDomain() const \
        { \
            return static_cast<EDomain>(Value); \
        } \
        \
    public: \
        static const char* GetLiteralByValue(int value) \
        { \
            switch (value) \
            { \
                PP_FOR_EACH(ENUM__STRING_BY_VALUE_ITEM, seq) \
                default: \
                    return NULL; \
            } \
        } \
        \
        static bool GetValueByLiteral(const char* literal, int* target) \
        { \
            PP_FOR_EACH(ENUM__VALUE_BY_STRING_ITEM, seq); \
            return false; \
        } \
        \
        static name FromString(const Stroka& str) \
        { \
            return MoveRV(FromString(str.c_str())); \
        } \
        \
        static name FromString(const char* str) \
        { \
            int value; \
            if (!GetValueByLiteral(str, &value)) { \
                ythrow yexception() \
                    << "Error parsing " PP_STRINGIZE(name) " value '" << str << "'"; \
            } else { \
                return MoveRV(name::SpawnFauxBase(value)); \
            } \
        } \
        \
        static bool FromString(const char* str, name* target) \
        { \
            int value; \
            if (!GetValueByLiteral(str, &value)) { \
                return false; \
            } else { \
                *target = MoveRV(name::SpawnFauxBase(value)); \
                return true; \
            } \
        }

//! EDomain declaration helper.
//! \{
#define ENUM__DOMAIN_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__DOMAIN_ITEM_SEQ, \
        ENUM__DOMAIN_ITEM_ATOMIC \
    )(item)()

#define ENUM__DOMAIN_ITEM_ATOMIC(item) \
    item PP_COMMA

#define ENUM__DOMAIN_ITEM_SEQ(seq) \
    PP_ELEMENT(seq, 0) = PP_ELEMENT(seq, 1) PP_COMMA
//! \}

//! #GetLiteralByValue() helper.
//! \{
#define ENUM__STRING_BY_VALUE_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__STRING_BY_VALUE_ITEM_SEQ, \
        ENUM__STRING_BY_VALUE_ITEM_ATOMIC \
    )(item)

#define ENUM__STRING_BY_VALUE_ITEM_SEQ(seq) \
    ENUM__STRING_BY_VALUE_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define ENUM__STRING_BY_VALUE_ITEM_ATOMIC(item) \
    case static_cast<int>(item): \
        return PP_STRINGIZE(item);
//! \}

//! #GetValueByLiteral() helper.
//! \{
#define ENUM__VALUE_BY_STRING_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__VALUE_BY_STRING_ITEM_SEQ, \
        ENUM__VALUE_BY_STRING_ITEM_ATOMIC \
    )(item)

#define ENUM__VALUE_BY_STRING_ITEM_SEQ(seq) \
    ENUM__VALUE_BY_STRING_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define ENUM__VALUE_BY_STRING_ITEM_ATOMIC(item) \
    if (::strcmp(literal, PP_STRINGIZE(item)) == 0) { \
        *target = static_cast<int>(item); \
        return true; \
    }
//! \}

//! Declaration of the relational operators; all at once.
#define ENUM__RELATIONAL_OPERATORS(name) \
    public: \
        ENUM__RELATIONAL_OPERATOR(name, < ) \
        ENUM__RELATIONAL_OPERATOR(name, > ) \
        ENUM__RELATIONAL_OPERATOR(name, <=) \
        ENUM__RELATIONAL_OPERATOR(name, >=) \
        ENUM__RELATIONAL_OPERATOR(name, ==) \
        ENUM__RELATIONAL_OPERATOR(name, !=)

//! Declaration of a single relational operator.
#define ENUM__RELATIONAL_OPERATOR(name, op) \
    bool operator op(const EDomain& e) const \
    { \
        return Value op static_cast<int>(e); \
    }

//! \}
//! \endinternal

////////////////////////////////////////////////////////////////////////////////

//! Declares a strongly-typed enumeration.
/*!
 * \param name Name of the enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define DECLARE_ENUM(name, seq) \
    BEGIN_DECLARE_ENUM(name, seq) \
    END_DECLARE_ENUM()

//! Begins the declaration of a strongly-typed enumeration.
//! See #DECLARE_ENUM.
#define BEGIN_DECLARE_ENUM(name, seq) \
    ENUM__CLASS(name, ::NYT::TEnumBase<name>, seq) \
    private: \
        static TBase SpawnFauxBase(int value) \
        { \
            return MoveRV(TBase(value)); \
        } \
        \
    public: \
        explicit name(int value) \
            : TBase(value) \
        { } \
        \
        Stroka ToString() const \
        { \
            const char* str = GetLiteralByValue(Value); \
            if (EXPECT_TRUE(str != NULL)) { \
                return str; \
            } else { \
                return Stroka::Join( \
                    PP_STRINGIZE(name) "(", ::ToString(Value), ")" \
                ); \
            } \
        } \
        \
        ENUM__RELATIONAL_OPERATORS(name)

//! Ends the declaration of a strongly-typed enumeration.
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

//! Declares a polymorphic enumeration with the specified scope.
/*!
 * \param scope Scope of the polymorphism.
 * \param name Name of the enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define DECLARE_POLY_ENUM2(name, scope, seq) \
    BEGIN_DECLARE_POLY_ENUM(name, scope, seq) \
    END_DECLARE_POLY_ENUM()

//! Begins the declaration of a polymorphic enumeration.
//! See #DECLARE_POLY_ENUM1.
#define BEGIN_DECLARE_POLY_ENUM(name, scope, seq) \
    ENUM__CLASS(name, ::NYT::TPolymorphicEnumBase<scope>, seq) \
    private: \
        static TBase SpawnFauxBase(int value, const char* literal = NULL) \
        { \
            return MoveRV(TBase( \
                value, literal ? literal : GetLiteralByValue(value) \
            )); \
        } \
        \
    public: \
        explicit name(int value, const char* literal = NULL) \
            : TBase(value, literal ? literal : GetLiteralByValue(value)) \
        { } \
        \
        Stroka ToString() const \
        { \
            if (Literal) { \
                return Literal; \
            } \
            \
            const char* str = name::GetLiteralByValue(Value); \
            if (EXPECT_TRUE(str != NULL)) { \
                return str; \
            } \
            \
            if (!TSameType<name, scope>::Result) { \
                str = scope::GetLiteralByValue(Value); \
                if (EXPECT_TRUE(str != NULL)) { \
                    return str; \
                } \
            } \
            \
            return Stroka::Join( \
                PP_STRINGIZE(name) "(", ::ToString(Value), ")" \
            ); \
        } \
        \
        ENUM__RELATIONAL_OPERATORS(name)

//! Ends the declaration of a polymorphic enumeration.
//! See #DECLARE_POLY_ENUM1.
#define END_DECLARE_POLY_ENUM() \
    }

/*! \} */

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

