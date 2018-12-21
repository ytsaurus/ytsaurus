#include "string.h"
#include "stream.h"
#include "null_consumer.h"
#include "parser.h"
#include "consumer.h"

#include <yt/core/misc/serialize.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonString::TYsonString()
    : Null_(true)
{ }

TYsonString::TYsonString(TString data, EYsonType type)
    : Null_(false)
    , Data_(std::move(data))
    , Type_(type)
{ }

TYsonString::TYsonString(const char* data, size_t length, EYsonType type)
    : Null_(false)
    , Data_(data, length)
    , Type_(type)
{ }

TYsonString::operator bool() const
{
    return !Null_;
}

const TString& TYsonString::GetData() const
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
    using NYT::Save;
    if (Null_) {
        Save(context, static_cast<i32>(-1));
    } else {
        Save(context, static_cast<i32>(Type_));
        Save(context, Data_);
    }
}

void TYsonString::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    // COMPAT(babenko)
    if (context.GetVersion() < 501) {
        Load(context, Data_);
        if (Data_.empty()) {
            Null_ = true;
        } else {
            Null_ = false;
            Type_ = EYsonType::Node;
        }
    } else {
        Type_ = static_cast<EYsonType>(Load<i32>(context));
        if (static_cast<i32>(Type_) == -1) {
            Null_ = true;
            Data_.clear();
        } else {
            Null_ = false;
            Load(context, Data_);
        }
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

TString ToString(const TYsonString& yson)
{
    return yson.GetData();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
