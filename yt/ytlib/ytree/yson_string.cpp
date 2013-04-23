#include "stdafx.h"
#include "yson_string.h"

#include "yson_stream.h"
#include "null_yson_consumer.h"

#include <ytlib/yson/parser.h>
#include <ytlib/yson/consumer.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void TYsonString::Validate() const
{
    TStringInput input(Data());
    ParseYson(TYsonInput(&input, GetType()), GetNullYsonConsumer());
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, NYson::IYsonConsumer* consumer)
{
    consumer->OnRaw(yson.Data(), yson.GetType());
}

void Save(TOutputStream* output, const TYsonString& ysonString)
{
    Save(output, ysonString.Data());
}

void Load(TInputStream* input, TYsonString& ysonString)
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
