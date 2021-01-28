#include "string.h"
#include "stream.h"
#include "null_consumer.h"
#include "parser.h"
#include "consumer.h"

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/variant.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonStringBuf::TYsonStringBuf()
{
    Type_ = EYsonType::Node; // fake
    Null_ = true;
}

TYsonStringBuf::TYsonStringBuf(const TYsonString& ysonString)
{
    if (ysonString) {
        Data_ = ysonString.AsStringBuf();
        Type_ = ysonString.GetType();
        Null_ = false;
    } else {
        Type_ = EYsonType::Node; // fake
        Null_ = true;
    }
}

TYsonStringBuf::TYsonStringBuf(const TString& data, EYsonType type)
    : TYsonStringBuf(TStringBuf(data), type)
{ }

TYsonStringBuf::TYsonStringBuf(TStringBuf data, EYsonType type)
    : Data_(data)
    , Type_(type)
    , Null_(false)
{ }

TYsonStringBuf::operator bool() const
{
    return !Null_;
}

TStringBuf TYsonStringBuf::AsStringBuf() const
{
    YT_VERIFY(*this);
    return Data_;
}

EYsonType TYsonStringBuf::GetType() const
{
    YT_VERIFY(*this);
    return Type_;
}

void TYsonStringBuf::Validate() const
{
    if (*this) {
        TMemoryInput input(Data_);
        ParseYson(TYsonInput(&input, Type_), GetNullYsonConsumer());
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedYsonStringData
    : public TRefCounted
    , public TWithExtraSpace<TRefCountedYsonStringData>
{
    char* GetData()
    {
        return static_cast<char*>(GetExtraSpacePtr());
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonString::TYsonString()
{
    Begin_ = nullptr;
    Size_ = 0;
    Type_ = EYsonType::Node; // fake
}

TYsonString::TYsonString(const TYsonStringBuf& ysonStringBuf)
{
    if (ysonStringBuf) {
        auto data = ysonStringBuf.AsStringBuf();
        auto payload = NewWithExtraSpace<TRefCountedYsonStringData>(data.length());
        ::memcpy(payload->GetData(), data.data(), data.length());
        Payload_ = payload;
        Begin_ = payload->GetData();
        Size_ = data.length();
        Type_ = ysonStringBuf.GetType();
    } else {
        Begin_ = nullptr;
        Size_ = 0;
        Type_ = EYsonType::Node; // fake
    }
}

TYsonString::TYsonString(
    TStringBuf data,
    EYsonType type)
    : TYsonString(TYsonStringBuf(data, type))
{ }

TYsonString::TYsonString(
    TString data,
    EYsonType type)
{
    Payload_ = data;
    Begin_ = data.data();
    Size_ = data.length();
    Type_ = type;
}

TYsonString::TYsonString(
    const TSharedRef& data,
    EYsonType type)
{
    Payload_ = data.GetHolder();
    Begin_ = data.Begin();
    Size_ = data.Size();
    Type_ = type;
}

TYsonString::operator bool() const
{
    return !std::holds_alternative<TNullPayload>(Payload_);
}

EYsonType TYsonString::GetType() const
{
    YT_VERIFY(*this);
    return Type_;
}

TStringBuf TYsonString::AsStringBuf() const
{
    YT_VERIFY(*this);
    return TStringBuf(Begin_, Begin_ + Size_);
}

TString TYsonString::ToString() const
{
    return Visit(
        Payload_,
        [] (const TNullPayload&) -> TString {
            YT_ABORT();
        },
        [&] (const TIntrusivePtr<TRefCounted>&) {
            return TString(AsStringBuf());
        },
        [] (const TString& payload) {
            return payload;
        });
}

size_t TYsonString::ComputeHash() const
{
    return THash<TStringBuf>()(TStringBuf(Begin_, Begin_ + Size_));
}

void TYsonString::Validate() const
{
    TYsonStringBuf(*this).Validate();
}

void TYsonString::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    if (*this) {
        Save(context, static_cast<i32>(Type_));
        TSizeSerializer::Save(context, Size_);
        TRangeSerializer::Save(context, TRef(Begin_, Size_));
    } else {
        Save(context, static_cast<i32>(-1));
    }
}

void TYsonString::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto type = Load<i32>(context);
    if (type != -1) {
        auto size = TSizeSerializer::Load(context);
        auto payload = NewWithExtraSpace<TRefCountedYsonStringData>(size);
        TRangeSerializer::Load(context, TMutableRef(payload->GetData(), size));
        Payload_ = payload;
        Begin_ = payload->GetData();
        Size_ = static_cast<i32>(size);
        Type_ = static_cast<EYsonType>(type);
    } else {
        Payload_ = TNullPayload();
        Begin_ = nullptr;
        Size_ = 0;
        Type_ = EYsonType::Node; // fake
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

void Serialize(const TYsonStringBuf& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

TString ToString(const TYsonString& yson)
{
    return yson.ToString();
}

TString ToString(const TYsonStringBuf& yson)
{
    return TString(yson.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
