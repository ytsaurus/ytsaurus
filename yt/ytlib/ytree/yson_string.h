#pragma once

#include "public.h"

#include <ytlib/yson/yson_consumer.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonString
{
public:
    explicit TYsonString(const Stroka& data, NYson::EYsonType type = NYson::EYsonType::Node):
        Data_(data), Type_(type)
    { }

    TYsonString()
    { }

    DEFINE_BYREF_RO_PROPERTY(Stroka, Data);
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

    void Validate() const;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, NYson::IYsonConsumer* consumer);
void Save(TOutputStream* output, const TYsonString& ysonString);
void Load(TInputStream* input, NYTree::TYsonString& ysonString);
bool operator == (const TYsonString& lhs, const TYsonString& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Direct conversion from Stroka to Node or Producer is forbidden.
//! For this case, use TRawString wrapper.
class TRawString
    : public Stroka
{
public:
    TRawString(const Stroka& str)
        : Stroka(str)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
