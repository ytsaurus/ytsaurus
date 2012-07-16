#pragma once

#include "common.h"
#include "guid.h"
#include "string.h"
#include "nullable.h"

#include <ytlib/object_server/public.h>
#include <ytlib/object_server/id.h>
#include <ytlib/cypress/public.h>
#include <ytlib/cypress_client/id.h>
#include <ytlib/ytree/yson_writer.h>

#include <tclap/CmdLine.h>

namespace TCLAP {

template <>
struct ArgTraits<Stroka>
{
    typedef StringLike ValueCategory;
};

template <>
struct ArgTraits< ::NYT::NObjectServer::TTransactionId >
{
    typedef ValueLike ValueCategory;
};

template <>
struct ArgTraits< ::NYT::NCypressClient::ELockMode >
{
    typedef ValueLike ValueCategory;
};

template <>
struct ArgTraits< ::NYT::NObjectServer::EObjectType >
{
    typedef ValueLike ValueCategory;
};

template <>
struct ArgTraits< ::NYT::NYTree::EYsonFormat >
{
    typedef ValueLike ValueCategory;
};

template <class T>
struct ArgTraits< NYT::TNullable<T> >
{
    typedef ValueLike ValueCategory;
};

}

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

inline std::istream& operator >> (std::istream& input, TGuid& guid)
{
    std::string s;
    input >> s;
    guid = TGuid::FromString(Stroka(s));
    return input;
}

template <class T>
inline std::istream& operator >> (std::istream& input, TEnumBase<T>& mode)
{
    std::string s;
    input >> s;
    mode = NYT::ParseEnum<T>(Stroka(s));
    return input;
}

template <class T>
inline std::istream& operator >> (std::istream& input, TNullable<T>& nullable)
{
    std::string s;
    input >> s;
    if (s.empty()) {
        nullable = NYT::TNullable<T>();
    } else {
        std::stringstream stream(s);
        T value;
        stream >> value;
        nullable = NYT::TNullable<T>(value);
    }
    return input;
}

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

