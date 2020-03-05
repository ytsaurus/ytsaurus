#include "string.h"
#include "stream.h"
#include "null_consumer.h"
#include "parser.h"
#include "consumer.h"

#include <yt/core/misc/serialize.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <typename TData>
TYsonStringBase<TData>::TYsonStringBase()
    : Null_(true)
{ }

template <typename TData>
TYsonStringBase<TData>::TYsonStringBase(TData data, EYsonType type)
    : Null_(false)
    , Data_(std::move(data))
    , Type_(type)
{ }

template <typename TData>
 TYsonStringBase<TData>::TYsonStringBase(const char* data, size_t length, EYsonType type)
    : Null_(false)
    , Data_(data, length)
    , Type_(type)
{ }

template <typename TData>
TYsonStringBase<TData>::operator bool() const
{
    return !Null_;
}

template <typename TData>
const TData& TYsonStringBase<TData>::GetData() const
{
    YT_VERIFY(!Null_);
    return Data_;
}

template <typename TData>
EYsonType TYsonStringBase<TData>::GetType() const
{
    YT_VERIFY(!Null_);
    return Type_;
}

template <typename TData>
void TYsonStringBase<TData>::Validate() const
{
    if (!Null_) {
        TMemoryInput input(Data_);
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
    Type_ = static_cast<EYsonType>(Load<i32>(context));
    if (static_cast<i32>(Type_) == -1) {
        Null_ = true;
        Data_.clear();
    } else {
        Null_ = false;
        Load(context, Data_);
    }
}

// Explicitly instantiate the template to link successfully.
template class TYsonStringBase<TString>;
template class TYsonStringBase<TStringBuf>;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonString& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

void Serialize(const TYsonStringBuf& yson, IYsonConsumer* consumer)
{
    consumer->OnRaw(yson);
}

template <typename TLeft, typename TRight>
bool Equals(const TLeft& lhs, const TRight& rhs)
{
    return lhs.GetData() == rhs.GetData() && lhs.GetType() == rhs.GetType();
}

bool operator == (const TYsonString& lhs, const TYsonString& rhs)
{
    return Equals(lhs, rhs);
}

bool operator == (const TYsonString& lhs, const TYsonStringBuf& rhs)
{
    return Equals(lhs, rhs);
}

bool operator == (const TYsonStringBuf& lhs, const TYsonString& rhs)
{
    return Equals(lhs, rhs);
}

bool operator == (const TYsonStringBuf& lhs, const TYsonStringBuf& rhs)
{
    return Equals(lhs, rhs);
}

bool operator != (const TYsonString& lhs, const TYsonString& rhs)
{
    return !(lhs == rhs);
}

bool operator != (const TYsonString& lhs, const TYsonStringBuf& rhs)
{
    return !(lhs == rhs);
}

bool operator != (const TYsonStringBuf& lhs, const TYsonString& rhs)
{
    return !(lhs == rhs);
}

bool operator != (const TYsonStringBuf& lhs, const TYsonStringBuf& rhs)
{
    return !(lhs == rhs);
}

TString ToString(const TYsonString& yson)
{
    return yson.GetData();
}

TString ToString(const TYsonStringBuf& yson)
{
    return TString{yson.GetData()};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
