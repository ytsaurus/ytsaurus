#include "stdafx.h"
#include "string.h"
#include "stream.h"
#include "null_consumer.h"
#include "parser.h"
#include "consumer.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonString::TYsonString()
    : Type_(EYsonType::Node)
{ }

TYsonString::TYsonString(const Stroka& data, EYsonType type)
    : Data_(data)
    , Type_(type)
{ }

void TYsonString::Validate() const
{
    TStringInput input(Data());
    ParseYson(TYsonInput(&input, GetType()), GetNullYsonConsumer());
}

void TYsonString::Save(TStreamSaveContext& context) const
{
    YCHECK(GetType() == EYsonType::Node);
    NYT::Save(context, Data_);
}

void TYsonString::Load(TStreamLoadContext& context)
{
    NYT::Load(context, Data_);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

bool operator == (const TYsonString& lhs, const TYsonString& rhs)
{
    return lhs.Data() == rhs.Data() && lhs.GetType() == rhs.GetType();
}

bool operator != (const TYsonString& lhs, const TYsonString& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
