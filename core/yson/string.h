#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonString
{
public:
    DEFINE_BYREF_RO_PROPERTY(Stroka, Data);
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);

public:
    //! Constructs a EYsonType::None instance.
    TYsonString();

    //! Constructs an instance with given type and content.
    explicit TYsonString(
        const Stroka& data,
        EYsonType type = EYsonType::Node);

    //! For types other than EYsonType::None, invokes the parser (which may throw).
    void Validate() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer);

bool operator == (const TYsonString& lhs, const TYsonString& rhs);
bool operator != (const TYsonString& lhs, const TYsonString& rhs);

Stroka ToString(const TYsonString& yson);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

//! A hasher for TYsonString
template <>
struct hash<NYT::NYson::TYsonString>
{
    size_t operator () (const NYT::NYson::TYsonString& str) const
    {
        return THash<Stroka>()(str.Data());
    }
};

////////////////////////////////////////////////////////////////////////////////
