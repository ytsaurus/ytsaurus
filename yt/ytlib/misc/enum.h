#pragma once

/*!
 * \file enum.h
 * \brief YT Enumerations
 */

/*!
 * \defgroup yt_enum Enumerations
 * \ingroup YT
 * \{
 */
#include "preprocessor.h"

#include <util/string/cast.h>
#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Base class for (more or less) type safe enumerations.
template<typename TDerived>
class TEnumBase
{
public:
    //! Default constructor.
    TEnumBase()
        : Value(0)
    { }

    //! (Explicit) constructor from integral values.
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

////////////////////////////////////////////////////////////////////////////////

//! \internal
//! Mix-ins for specifying enumerations.
//! \{

//! Base mix-in.
/*!
 * Base mix-in declares enumeration domain, default constructor,
 * constructor from a domain value and implicit conversion operator
 * to the enumeration domain.
 */
#define MIXIN_ENUM__BASE(basename, name, seq) \
    public: \
        enum EDomain \
        { \
            PP_FOR_EACH(DECLARE_ENUM__DOMAIN_ITEM, seq) \
        }; \
        \
        name() \
        { } \
        \
        name(const EDomain& e) \
        { \
            Value = static_cast<int>(e); \
        } \
        \
        operator EDomain() const \
        { \
            return static_cast<EDomain>(Value); \
        } \
        \
        name& operator=(const basename::EDomain& e) \
        { \
            Value = static_cast<int>(e); \
            return *this; \
        } \
    private: \
        friend class ::NYT::TEnumBase<basename>;

//! EDomain declaration helper.
// \{
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
// \}

//! Mix-in with implicit constructor from integral value.
#define MIXIN_ENUM__IMPLICIT_INT_CONSTRUCTOR(name) \
    public: \
        name(int value) \
        { \
            Value = value; \
        }

//! Mix-in with explicit constructor from integral value.
#define MIXIN_ENUM__EXPLICIT_INT_CONSTRUCTOR(name) \
    public: \
        explicit name(int value) \
        { \
            Value = value; \
        }

//! ToString() mix-in.
// \{
#define MIXIN_ENUM__TO_STRING(basename, name, seq, derived) \
    public: \
        Stroka ToString() const \
        { \
            switch (Value) \
            { \
                PP_FOR_EACH(DECLARE_ENUM__TO_STRING_ITEM, seq) \
                default: \
                    PP_IF( \
                        derived, \
                        MIXIN_ENUM__TO_STRING__DERIVED_RETURN, \
                        MIXIN_ENUM__TO_STRING__DEFAULT_RETURN  \
                    )(basename, name) \
                    ; \
            } \
        }

#define MIXIN_ENUM__TO_STRING__DERIVED_RETURN(basename, name) \
    return basename::ToString()

#define MIXIN_ENUM__TO_STRING__DEFAULT_RETURN(basename, name) \
    return PP_STRINGIZE(name) "(" + ::ToString(Value) + ")"
// \}

//! ToString() declaration helper.
// \{
#define DECLARE_ENUM__TO_STRING_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        DECLARE_ENUM__TO_STRING_ITEM_SEQ, \
        DECLARE_ENUM__TO_STRING_ITEM_ATOMIC \
    )(item)

#define DECLARE_ENUM__TO_STRING_ITEM_ATOMIC(item) \
    case static_cast<int>(item): \
        return PP_STRINGIZE(item);

#define DECLARE_ENUM__TO_STRING_ITEM_SEQ(seq) \
    DECLARE_ENUM__TO_STRING_ITEM_ATOMIC(PP_ELEMENT(seq, 0))
//! \}

//! FromString() mix-in.
// \{
#define MIXIN_ENUM__FROM_STRING(basename, name, seq, derived) \
    public: \
        static name FromString(const Stroka& str) \
        { \
            name target; \
            if (!FromString(str, &target)) { \
                ythrow yexception() << "Error parsing enumeration value '" << str << "'"; \
            } \
            return target; \
        } \
        \
        static bool FromString(const Stroka& str, name* target) \
        { \
            PP_FOR_EACH(DECLARE_ENUM__FROM_STRING_ITEM, seq) \
            PP_IF( \
                derived, \
                MIXIN_ENUM__FROM_STRING__DERIVED_RETURN, \
                MIXIN_ENUM__FROM_STRING__DEFAULT_RETURN  \
            )(basename, name) \
            ; \
        }

#define MIXIN_ENUM__FROM_STRING__DEFAULT_RETURN(basename, name) \
    return false

#define MIXIN_ENUM__FROM_STRING__DERIVED_RETURN(basename, name) \
    return basename::FromString(str, reinterpret_cast<basename*>(target))
// \}

//! FromString() declaration helper.
// \{
#define DECLARE_ENUM__FROM_STRING_ITEM(item) \
    PP_IF( \
        PP_IS_SEQUENCE(item), \
        DECLARE_ENUM__FROM_STRING_ITEM_SEQ, \
        DECLARE_ENUM__FROM_STRING_ITEM_ATOMIC \
    )(item)

#define DECLARE_ENUM__FROM_STRING_ITEM_ATOMIC(item) \
    if (str == PP_STRINGIZE(item)) { \
        *target = item; \
        return true; \
    }

#define DECLARE_ENUM__FROM_STRING_ITEM_SEQ(seq) \
    DECLARE_ENUM__FROM_STRING_ITEM_ATOMIC(PP_ELEMENT(seq, 0))
// \}

//! \endinternal
//! \}

//! Begins a new enumeration declaration.
/*!
 * \param name Name of the enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define BEGIN_DECLARE_ENUM(name, seq) \
    class name : public ::NYT::TEnumBase<name> \
    { \
        MIXIN_ENUM__BASE(name, name, seq) \
        MIXIN_ENUM__TO_STRING(name, name, seq, PP_FALSE) \
        MIXIN_ENUM__FROM_STRING(name, name, seq, PP_FALSE)

//! Begins a new derived enumeration declaration.
/*!
 * \param basename Name of the base enumeration.
 * \param name Name of the derived enumeration.
 * \param seq Enumeration domain encoded as a <em>sequence</em>.
 */
#define BEGIN_DECLARE_DERIVED_ENUM(basename, name, seq) \
    class name : public basename \
    { \
        MIXIN_ENUM__BASE(basename, name, seq) \
        MIXIN_ENUM__TO_STRING(basename, name, seq, PP_TRUE) \
        MIXIN_ENUM__FROM_STRING(basename, name, seq, PP_TRUE)

//! End an enumeration declaration.
#define END_DECLARE_ENUM() \
    }

//! Declares an enumeration with explicit integral conversion and no custom methods.
#define DECLARE_ENUM(name, seq) \
    BEGIN_DECLARE_ENUM(name, seq) \
        MIXIN_ENUM__EXPLICIT_INT_CONSTRUCTOR(name) \
    END_DECLARE_ENUM()

//! Declares a derived enumeration with explicit integral conversion and no custom methods.
#define DECLARE_DERIVED_ENUM(basename, name, seq) \
    BEGIN_DECLARE_DERIVED_ENUM(basename, name, seq) \
        MIXIN_ENUM__EXPLICIT_INT_CONSTRUCTOR(name) \
    END_DECLARE_ENUM()

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

//! \}

