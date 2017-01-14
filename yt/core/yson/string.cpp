#include "string.h"
#include "stream.h"
#include "null_consumer.h"
#include "parser.h"
#include "consumer.h"

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonString::TYsonString()
    : Null_(true)
{ }

TYsonString::TYsonString(const Stroka& data, EYsonType type)
    : Null_(false)
    , Data_(data)
    , Type_(type)
{ }

TYsonString::operator bool() const
{
    return !Null_;
}

const Stroka& TYsonString::GetData() const
{
    YCHECK(!Null_);
    return Data_;
}

EYsonType TYsonString::GetType() const
{
    YCHECK(!Null_);
    return Type_;
}

void TYsonString::Validate() const
{
    if (!Null_) {
        TStringInput input(Data_);
        ParseYson(TYsonInput(&input, Type_), GetNullYsonConsumer());
    }
}

void TYsonString::Save(TStreamSaveContext& context) const
{
    // XXX(babenko): what about empty fragments?
    NYT::Save(context, Data_);
}

void TYsonString::Load(TStreamLoadContext& context)
{
    NYT::Load(context, Data_);
    if (Data_.empty()) {
        Null_ = true;
    } else {
        Null_ = false;
        Type_ = EYsonType::Node;
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

bool operator == (const TYsonString& lhs, const TYsonString& rhs)
{
    return lhs.GetData() == rhs.GetData() && lhs.GetType() == rhs.GetType();
}

bool operator != (const TYsonString& lhs, const TYsonString& rhs)
{
    return !(lhs == rhs);
}

Stroka ToString(const TYsonString& yson)
{
    return yson.GetData();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
