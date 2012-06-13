#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EYsonType,
    (Node)
    (ListFragment)
    (MapFragment)
);

////////////////////////////////////////////////////////////////////////////////

class TYsonString
{
public:
    explicit TYsonString(const Stroka& data, EYsonType type = EYsonType::Node):
        Data_(data), Type_(type)
    { }

    TYsonString()
    { }

    DEFINE_BYREF_RO_PROPERTY(Stroka, Data);
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);

    void Validate() const;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer);
void Save(TOutputStream* output, const NYTree::TYsonString& ysonString);
void Load(TInputStream* input, NYTree::TYsonString& ysonString);
bool operator == (const TYsonString& lhs, const TYsonString& rhs);

////////////////////////////////////////////////////////////////////////////////

}} // namespace NYT::NYTree
