#pragma once

#include "common.h"
#include "guid.h"
#include "enum.h"
#include "string.h"

#include <ytlib/object_server/public.h>
#include <ytlib/object_server/id.h>
#include <ytlib/cypress/public.h>
#include <ytlib/cypress/id.h>
#include <ytlib/ytree/yson_writer.h>

#include <tclap/CmdLine.h>

namespace TCLAP {

template<>
struct ArgTraits<Stroka>
{
    typedef StringLike ValueCategory;
};

template<>
struct ArgTraits<NYT::NObjectServer::TTransactionId>
{
    typedef ValueLike ValueCategory;
};

template<>
struct ArgTraits<NYT::NCypress::ELockMode>
{
    typedef ValueLike ValueCategory;
};

template<>
struct ArgTraits<NYT::NObjectServer::EObjectType>
{
    typedef ValueLike ValueCategory;
};

template<>
struct ArgTraits<NYT::NYTree::EYsonFormat>
{
    typedef ValueLike ValueCategory;
};

}

////////////////////////////////////////////////////////////////////////////////

inline std::istream& operator >> (std::istream& input, NYT::TGuid& guid)
{
    std::string s;
    input >> s;
    guid = NYT::TGuid::FromString(Stroka(s));
    return input;
}

template<class T>
inline std::istream& operator >> (std::istream& input, NYT::TEnumBase<T>& mode)
{
    std::string s;
    input >> s;
    mode = NYT::ParseEnum<T>(Stroka(s));
    return input;
}

////////////////////////////////////////////////////////////////////////////////

