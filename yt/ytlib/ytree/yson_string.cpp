#include "stdafx.h"
#include "yson_string.h"

#include "yson_stream.h"
#include "yson_parser.h"
#include "yson_consumer.h"
#include "null_yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void TYsonString::Validate() const
{
    TStringInput input(Data());
    ParseYson(TYsonInput(&input, GetType()), GetNullYsonConsumer());
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson.Data(), yson.GetType());
}

void Save(TOutputStream* output, const NYTree::TYsonString& ysonString)
{
    Save(output, ysonString.Data());
}

void Load(TInputStream* input, NYTree::TYsonString& ysonString)
{
    Stroka str;
    Load(input, str);
    ysonString = NYTree::TYsonString(str);
}

bool operator == (const TYsonString& lhs, const TYsonString& rhs)
{
    return lhs.Data() == rhs.Data() && lhs.GetType() == rhs.GetType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
