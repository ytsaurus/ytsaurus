#include "stdafx.h"
#include "yson_string.h"
#include "yson_stream.h"
#include "null_yson_consumer.h"

#include <core/yson/parser.h>
#include <core/yson/consumer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

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
    consumer->OnRaw(yson.Data(), yson.GetType());
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

} // namespace NYTree
} // namespace NYT
