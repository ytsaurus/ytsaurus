#pragma once

/*!
 * \file enum.h
 * \brief Smart enumerations
 */

#include "preprocessor.h"

#include <util/stream/output.h>

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

//! Base class tag for strongly-typed enumerations.
template <class T>
class TEnumBase
{ };

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
#define ENUM__CLASS(name, seq) \
    class name \
        : public ::NYT::TEnumBase<name> \
    { \
    public: \
        typedef name TThis; \
        \
        enum EDomain \
        { \
            PP_FOR_EACH(ENUM__DOMAIN_ITEM, seq) \
        }; \
        \
        name() \
            : Value(static_cast<EDomain>(0)) \
        { } \
        \
        name(EDomain e) \
            : Value(e) \
        { } \
        \
        explicit name(int value) \
            : Value(static_cast<EDomain>(value)) \
        { } \
        \
        name& operator=(EDomain e) \
        { \
            Value = e; \
            return *this; \
        } \
        \
        operator EDomain() const \
        { \
            return Value; \
        } \
        \
        static const TStringBuf& GetTypeName() \
        { \
            static TStringBuf typeName = STRINGBUF(PP_STRINGIZE(name)); \
            return typeName; \
        } \
        \
        friend TOutputStream& operator << (TOutputStream& stream, name value) \
        { \
            const auto* literal = GetLiteralByValue(value.Value); \
            if (literal) { \
                stream << *literal; \
            } else { \
                stream << GetTypeName() << "(" << static_cast<int>(value.Value) << ")"; \
            } \
            return stream; \
        } \
        \
        static const TStringBuf* GetLiteralByValue(int value) \
        { \
            switch (value) \
            { \
                PP_FOR_EACH(ENUM__LITERAL_BY_VALUE_ITEM, seq) \
                default: \
                    return nullptr; \
            } \
        } \
        \
        static bool GetValueByLiteral(const TStringBuf& literal, int* result) \
        { \
            PP_FOR_EACH(ENUM__VALUE_BY_LITERAL_ITEM, seq); \
            return false; \
        } \
        \
        static constexpr int GetDomainSize() \
        { \
            return PP_COUNT(seq); \
        } \
        \
        static const std::vector<name>& GetDomainValues() \
        { \
            static name values[] = { \
                PP_FOR_EACH(ENUM__GET_DOMAIN_VALUES_ITEM, seq) \
            }; \
            static std::vector<name> result(values, values + GetDomainSize()); \
            return result; \
        } \
        \
        static const std::vector<Stroka>& GetDomainNames() \
        { \
            static Stroka values[] = { \
                PP_FOR_EACH(ENUM__GET_DOMAIN_NAMES_ITEM, seq) \
            }; \
            static std::vector<Stroka> result(values, values + GetDomainSize()); \
            return result; \
        } \
        \
        static name FromString(const TStringBuf& str) \
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
        static bool FromString(const TStringBuf& str, name* result) \
        { \
            int value; \
            if (!GetValueByLiteral(str, &value)) { \
                return false; \
            } else { \
                *result = name(value); \
                return true; \
            } \
        } \
        \
    private: \
        EDomain Value;

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
    case static_cast<int>(item): { \
        static const TStringBuf literal = STRINGBUF(PP_STRINGIZE(item)); \
        return &literal; \
    } \
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
    if (literal == PP_STRINGIZE(item)) { \
        *result = static_cast<int>(item); \
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
    TThis(item),
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
    Stroka(PP_STRINGIZE(item)),
//! \}

//! Declaration of relational operators; all at once.
#define ENUM__RELATIONAL_OPERATORS(name) \
    public: \
        ENUM__RELATIONAL_OPERATOR(name, < ) \
        ENUM__RELATIONAL_OPERATOR(name, > ) \
        ENUM__RELATIONAL_OPERATOR(name, <=) \
        ENUM__RELATIONAL_OPERATOR(name, >=) \

//! Declaration of equality operators; all at once.
#define ENUM__EQUALITY_OPERATORS(name) \
    public: \
        ENUM__RELATIONAL_OPERATOR(name, ==) \
        ENUM__RELATIONAL_OPERATOR(name, !=)

//! Declaration of a single relational operator.
#define ENUM__RELATIONAL_OPERATOR(name, op) \
    bool operator op(EDomain other) const \
    { \
        return static_cast<int>(Value) op static_cast<int>(other); \
    }

//! Declaration of bitwise operators; all at once.
#define ENUM__BITWISE_OPERATORS(name) \
    public: \
        ENUM__BITWISE_OPERATOR(name, &=, & ) \
        ENUM__BITWISE_OPERATOR(name, |=, | ) \
        ENUM__BITWISE_OPERATOR(name, ^=, ^ ) \

//! Declaration of a single bitwise operator (together with its assignment version).
#define ENUM__BITWISE_OPERATOR(name, assignOp, op) \
    name operator op (EDomain other) const \
    { \
        return name(static_cast<int>(Value) op static_cast<int>(other)); \
    } \
    \
    name& operator assignOp (EDomain other) \
    { \
        Value = EDomain(static_cast<int>(Value) op static_cast<int>(other)); \
        return *this; \
    }

//! #Decompose() helper.
//! \{
#define ENUM__DECOMPOSE(name, seq) \
    public: \
        std::vector<name> Decompose() const \
        { \
            std::vector<name> result; \
            PP_FOR_EACH(ENUM__DECOMPOSE_ITEM, seq) \
            return result; \
        }

#define ENUM__DECOMPOSE_ITEM(item) \
    ENUM__DECOMPOSE_ITEM_SEQ(PP_ELEMENT(item, 0))

#define ENUM__DECOMPOSE_ITEM_SEQ(itemName) \
    if (Value & itemName) { \
        result.push_back(itemName); \
    }
//! \}

#define BEGIN_DECLARE_ENUM(name, seq) \
    ENUM__CLASS(name, seq)

#define END_DECLARE_ENUM() \
    }

//! \}
//! \endinternal

////////////////////////////////////////////////////////////////////////////////

//! Declares a strongly-typed enumeration.
/*!
 * \param name Enumeration name.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define DECLARE_ENUM(name, seq) \
    BEGIN_DECLARE_ENUM(name, seq) \
        ENUM__EQUALITY_OPERATORS(name) \
        ENUM__RELATIONAL_OPERATORS(name) \
    END_DECLARE_ENUM()

//! Declares a strongly-typed flagged enumeration.
/*!
 * \param name Enumeration name.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define DECLARE_FLAGGED_ENUM(name, seq) \
    BEGIN_DECLARE_ENUM(name, seq) \
        ENUM__EQUALITY_OPERATORS(name) \
        ENUM__BITWISE_OPERATORS(name) \
        ENUM__DECOMPOSE(name, seq) \
    END_DECLARE_ENUM()

/*! \} */

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

