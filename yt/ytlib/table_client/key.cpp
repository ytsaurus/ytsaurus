#include "stdafx.h"
#include "key.h"

#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TKeyPart::TKeyPart ()
    : Type(EKeyType::Null)
{ }

TKeyPart::TKeyPart(const TStringBuf& value)
    : StrValue(value)
    , Type(EKeyType::String)
{ }

TKeyPart::TKeyPart(i64 value)
    : IntValue(value)
    , Type(EKeyType::Integer)
{ }

TKeyPart::TKeyPart(double value)
    : DoubleValue(value)
    , Type(EKeyType::Double)
{ }

TKeyPart TKeyPart::CreateComposite()
{
    TKeyPart keyPart;
    keyPart.Type = EKeyType::Composite;
    return keyPart;
}

i64 TKeyPart::GetInteger() const
{
    YASSERT(Type == EKeyType::Integer);
    return IntValue;
}

double TKeyPart::GetDouble() const
{
    YASSERT(Type == EKeyType::Double);
    return DoubleValue;
}

const TStringBuf& TKeyPart::GetString() const
{
    YASSERT(Type == EKeyType::String);
    return StrValue;
}

Stroka TKeyPart::ToString() const
{
    switch (Type) {
    case EKeyType::Null:
        return "[Null]";
    case EKeyType::Composite:
        return "[Composite]";
    case EKeyType::String:
        return StrValue.ToString();
    case EKeyType::Integer:
        return ::ToString(IntValue);
    case EKeyType::Double:
        return ::ToString(DoubleValue);
    default:
        YUNREACHABLE();
    }
}

/*
size_t TKeyPart::GetSize() const
{
    switch (Type) {
    case EKeyType::Null:
    case EKeyType::Composite:
        return sizeof(Type);

    case EKeyType::String:
        return sizeof(Type) + StrValue.Length();

    case EKeyType::Integer:
        return sizeof(Type) + sizeof(IntValue);

    case EKeyType::Double:
        return sizeof(Type) + sizeof(DoubleValue);

    default:
        YUNREACHABLE();
    }
}
*/

NProto::TKeyPart TKeyPart::ToProto() const
{
    NProto::TKeyPart keyPart;
    keyPart.set_type(Type);

    switch (Type) {
    case EKeyType::String:
        keyPart.set_str_value(StrValue.begin(), StrValue.size());
        break;

    case EKeyType::Integer:
        keyPart.set_int_value(IntValue);
        break;

    case EKeyType::Double:
        keyPart.set_double_value(DoubleValue);
        break;

    //default: do nothing.
    }

    return keyPart;
}

int CompareKeyParts(const TKeyPart& rhs, const TKeyPart& lhs)
{
    if (rhs.GetType() == lhs.GetType()) {
        switch (rhs.GetType()) {
        case EKeyType::String: {
            auto minSize = std::min(lhs.GetString().size(), rhs.GetString().size());
            auto res = strncmp(lhs.GetString().begin(), rhs.GetString().end(), minSize);
            return res == 0 ? static_cast<int>(lhs.GetString().size()) - 
                    static_cast<int>(rhs.GetString().size()) : res;
        }

        case EKeyType::Integer:
            return rhs.GetInteger() - lhs.GetInteger();

        case EKeyType::Double:
            if (rhs.GetDouble() > lhs.GetDouble())
                return 1;
            if (rhs.GetDouble() < lhs.GetDouble())
                return -1;
            return 0;

        default:
            return true; // All composites are equal to each other.
        }
    } else 
        return int(rhs.GetType()) - int(rhs.GetType());
}

////////////////////////////////////////////////////////////////////////////////

TKey::TKey(int columnCount, int size)
    : MaxSize(size)
    , ColumnCount(size)
    , CurrentSize(0)
    , Buffer(size)
    , Parts(ColumnCount)
{ }

void TKey::Reset()
{
    CurrentSize = 0;
    Buffer.Clear();
    Parts.clear();
    Parts.resize(ColumnCount);
}

template <class T>
void TKey::AddValue(int index, const T& value)
{
    YASSERT(index < ColumnCount);
    int size = sizeof(EKeyType) + sizeof(value);
    if (CurrentSize + size < MaxSize) {
        Parts[index] = TKeyPart(value);
        CurrentSize += size;
    } else 
        CurrentSize = MaxSize;
}

template <>
void TKey::AddValue(int index, const TStringBuf& value)
{
    YASSERT(index < ColumnCount);
    // Strip long key values.
    int freeSize = MaxSize - CurrentSize;
    int length = std::min(freeSize - sizeof(EKeyType), value.size());

    if (length > 0) {
        auto begin = Buffer.Begin() + Buffer.GetSize();
        Buffer.Write(value.begin(), length);

        Parts[index] = TKeyPart(TStringBuf(begin, length));
        CurrentSize += length + sizeof(EKeyType);
    } else 
        CurrentSize = MaxSize;
}

void TKey::AddComposite(int index)
{
    YASSERT(index < ColumnCount);
    int size = sizeof(EKeyType);
    if (CurrentSize + size < MaxSize) {
        Parts[index] = TKeyPart::CreateComposite();
        CurrentSize += size;
    } else 
        CurrentSize = MaxSize;
}

void TKey::Swap(TKey& other)
{
    // May be one day these asserts will be deprecated.
    YASSERT(MaxSize == other.MaxSize);
    YASSERT(ColumnCount == other.ColumnCount);

    Parts.swap(other.Parts);
    Buffer.Swap(other.Buffer);

    {
        int tmp = CurrentSize;
        CurrentSize = other.CurrentSize;
        other.CurrentSize = tmp;
    }
}

void TKey::SetColumnCount(int columnCount)
{
    ColumnCount = columnCount;
}

Stroka TKey::ToString() const
{
    return JoinToString(Parts);
}

NProto::TKey TKey::ToProto() const
{
    NProto::TKey key;
    FOREACH(const auto& part, Parts) {
        *key.add_parts() = part.ToProto();
    }
    return key;
}

bool TKey::operator<(const TKey& other) const
{
    int minSize = std::min(Parts.size(), other.Parts.size());
    for (int i = 0; i < minSize; ++i) {
        int res = CompareKeyParts(Parts[i], other.Parts[i]);
        if (res < 0)
            return true;
        if (res > 0)
            return false;
    }

    if (Parts.size() < other.Parts.size())
        return true;

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
