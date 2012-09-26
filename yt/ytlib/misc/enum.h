#pragma once

/*!
 * \file enum.h
 * \brief Smart enumerations
 */

#include "preprocessor.h"
#include "rvalue.h"

#include <util/stream/base.h>
#include <util/string/cast.h>
#include <util/generic/typehelpers.h>
#include <util/generic/vector.h>
#include <util/ysaveload.h>

#include <stdexcept>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * \defgroup yt_enum Smart enumerations
 * \ingroup yt_commons
 *
 * \{
 *
 * A string literal could be associated with an instance of polymorphic
 * enumeration and this literal is preserved during casts. 
 *
 * \page yt_enum_examples Examples
 * Please refer to the unit test for an actual example of usage
 * (unittests/enum_ut.cpp).
 *
 */

//! Base class for strongly-typed enumerations.
template <class TDerived>
class TEnumBase
{
public:
    //! Default constructor.
    TEnumBase()
        : Value(0)
    { }

    //! Explicit constructor.
    explicit TEnumBase(int value)
        : Value(value)
    { }

    //! Returns the underlying integral value.
    int ToValue() const
    {
        return Value;
    }

    void Load(TInputStream* input)
    {
        i32 value;
        ::Load(input, value);
        Value = value;
    }

    void Save(TOutputStream* output) const
    {
        i32 value = static_cast<i32>(Value);
        ::Save(output, value);
    }

protected:
    int Value;
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
        name(const EDomain& e) \
            : TBase(static_cast<int>(e)) \
        { } \
        \
        name& operator=(const EDomain& e) \
        { \
            name::operator=(name(static_cast<int>(e))); \
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
                PP_FOR_EACH(ENUM__LITERAL_BY_VALUE_ITEM, seq) \
                default: \
                    return NULL; \
            } \
        } \
        \
        static bool GetValueByLiteral(const char* literal, int* target) \
        { \
            PP_FOR_EACH(ENUM__VALUE_BY_LITERAL_ITEM, seq); \
            return false; \
        } \
        \
        static int GetDomainSize() \
        { \
            return PP_COUNT(seq); \
        } \
        \
        static std::vector<EDomain> GetDomainValues() \
        { \
            static const EDomain values[] = { \
                PP_FOR_EACH(ENUM__GET_DOMAIN_VALUES_ITEM, seq) \
                static_cast<EDomain>(-1) \
            }; \
            return std::vector<EDomain>(values, values + sizeof(values) / sizeof(values[0]) - 1); \
        } \
        \
        static std::vector<Stroka> GetDomainNames() \
        { \
            static const char* names[] = { \
                PP_FOR_EACH(ENUM__GET_DOMAIN_NAMES_ITEM, seq) \
                NULL \
            }; \
            return std::vector<Stroka>(names, names + sizeof(names) / sizeof(names[0]) - 1); \
        } \
        \
        static name FromString(const char* str) \
        { \
            int value; \
            if (!GetValueByLiteral(str, &value)) { \
                throw std::runtime_error(Sprintf("Error parsing %s value %s", \
                    PP_STRINGIZE(name), \
                    ~Stroka(str).Quote())); \
            } \
            return name(value); \
        } \
        \
        static name FromString(const Stroka& str) \
        { \
            return name::FromString(str.c_str()); \
        } \
        \
        static bool FromString(const char* str, name* target) \
        { \
            int value; \
            if (!GetValueByLiteral(str, &value)) { \
                return false; \
            } else { \
                *target = name(value); \
                return true; \
            } \
        } \
        \
        static bool FromString(const Stroka& str, name* target) \
        { \
            return name::FromString(str.c_str(), target); \
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
#define ENUM__LITERAL_BY_VALUE_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__LITERAL_BY_VALUE_ITEM_SEQ, \
        ENUM__LITERAL_BY_VALUE_ITEM_ATOMIC \
    )(item)

#define ENUM__LITERAL_BY_VALUE_ITEM_SEQ(seq) \
    ENUM__LITERAL_BY_VALUE_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define ENUM__LITERAL_BY_VALUE_ITEM_ATOMIC(item) \
    case static_cast<int>(item): \
        return PP_STRINGIZE(item);
//! \}

//! #GetValueByLiteral() helper.
//! \{
#define ENUM__VALUE_BY_LITERAL_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__VALUE_BY_LITERAL_ITEM_SEQ, \
        ENUM__VALUE_BY_LITERAL_ITEM_ATOMIC \
    )(item)

#define ENUM__VALUE_BY_LITERAL_ITEM_SEQ(seq) \
    ENUM__VALUE_BY_LITERAL_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define ENUM__VALUE_BY_LITERAL_ITEM_ATOMIC(item) \
    if (::strcmp(literal, PP_STRINGIZE(item)) == 0) { \
        *target = static_cast<int>(item); \
        return true; \
    }
//! \}

//! #GetDomainValues() helper.
//! \{
#define ENUM__GET_DOMAIN_VALUES_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__GET_DOMAIN_VALUES_ITEM_SEQ, \
        ENUM__GET_DOMAIN_VALUES_ITEM_ATOMIC \
    )(item)

#define ENUM__GET_DOMAIN_VALUES_ITEM_SEQ(seq) \
    ENUM__GET_DOMAIN_VALUES_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define ENUM__GET_DOMAIN_VALUES_ITEM_ATOMIC(item) \
    (item),
//! \}

//! #GetDomainNames() helper.
//! {
#define ENUM__GET_DOMAIN_NAMES_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        ENUM__GET_DOMAIN_NAMES_ITEM_SEQ, \
        ENUM__GET_DOMAIN_NAMES_ITEM_ATOMIC \
    )(item)

#define ENUM__GET_DOMAIN_NAMES_ITEM_SEQ(seq) \
    ENUM__GET_DOMAIN_NAMES_ITEM_ATOMIC(PP_ELEMENT(seq, 0))

#define ENUM__GET_DOMAIN_NAMES_ITEM_ATOMIC(item) \
    PP_STRINGIZE(item), \
//! \}

//! Declaration of relational operators; all at once.
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
    public: \
        explicit name(int value) \
            : TBase(value) \
        { } \
        \
        Stroka ToString() const \
        { \
            Stroka str = GetLiteralByValue(Value); \
            if (LIKELY(!str.empty())) { \
                return str; \
            } else { \
                return Stroka(PP_STRINGIZE(name)) + "(" + ::ToString(Value)+ ")"; \
            } \
        } \
        ENUM__RELATIONAL_OPERATORS(name)

//! Ends the declaration of a strongly-typed enumeration.
//! See #DECLARE_ENUM.
#define END_DECLARE_ENUM() \
    }

/*! \} */

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

