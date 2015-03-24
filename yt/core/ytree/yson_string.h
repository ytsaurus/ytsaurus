#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/property.h>

#include <core/yson/public.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonString
{
public:
    DEFINE_BYREF_RO_PROPERTY(Stroka, Data);
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

public:
    TYsonString();

    explicit TYsonString(
        const Stroka& data,
        NYson::EYsonType type = NYson::EYsonType::Node);

    void Validate() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, NYson::IYsonConsumer* consumer);

bool operator == (const TYsonString& lhs, const TYsonString& rhs);
bool operator != (const TYsonString& lhs, const TYsonString& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

//! A hasher for TYsonString
template <>
struct hash<NYT::NYTree::TYsonString>
{
    size_t operator () (const NYT::NYTree::TYsonString& str) const
    {
        return THash<Stroka>()(str.Data());
    }
};

////////////////////////////////////////////////////////////////////////////////
