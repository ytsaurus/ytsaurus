#pragma once

#include "guid.h"
#include <ytlib/object_server/public.h>
#include <ytlib/cypress/public.h>

namespace TCLAP {

template<>
struct ArgTraits<Stroka> {
    typedef StringLike ValueCategory;
};

template<>
struct ArgTraits<NYT::NObjectServer::TTransactionId> {
    typedef ValueLike ValueCategory;
};

template<>
struct ArgTraits<NYT::NCypress::ELockMode> {
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

inline std::istream& operator >> (std::istream& input, NYT::NCypress::ELockMode& mode)
{
    std::string s;
    input >> s;
    mode = NYT::NCypress::ELockMode::FromString(Stroka(s));
    return input;
}

////////////////////////////////////////////////////////////////////////////////

