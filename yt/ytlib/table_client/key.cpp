#include "stdafx.h"
#include "key.h"

#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TKeyPart::TKeyPart ()
    : Type_(EKeyType::Null)
{ }

TKeyPart::TKeyPart(size_t offset, int length, const TBlob* buffer)
    : StrOffset(offset)
    , StrLength(length)
    , Buffer(buffer)
    , Type_(EKeyType::String)
{ }

TKeyPart::TKeyPart(i64 value)
    : IntValue(value)
    , Type_(EKeyType::Integer)
{ }

TKeyPart::TKeyPart(double value)
    : DoubleValue(value)
    , Type_(EKeyType::Double)
{ }

TKeyPart TKeyPart::CreateComposite()
{
    TKeyPart keyPart;
    keyPart.Type_ = EKeyType::Composite;
    return keyPart;
}

i64 TKeyPart::GetInteger() const
{
    YASSERT(Type_ == EKeyType::Integer);
    return IntValue;
}

double TKeyPart::GetDouble() const
{
    YASSERT(Type_ == EKeyType::Double);
    return DoubleValue;
}

TStringBuf TKeyPart::GetString() const
{
    YASSERT(Type_ == EKeyType::String);
    return TStringBuf(Buffer->begin() + StrOffset, StrLength);
}

Stroka ToString(const TKeyPart& keyPart)
{
    switch (keyPart.GetType()) {
    case EKeyType::Null:
        return "[Null]";
    case EKeyType::Composite:
        return "[Composite]";
    case EKeyType::String:
        return keyPart.GetString().ToString();
    case EKeyType::Integer:
        return ::ToString(keyPart.GetInteger());
    case EKeyType::Double:
        return ::ToString(keyPart.GetDouble());
    default:
        YUNREACHABLE();
    }
}

NProto::TKeyPart TKeyPart::ToProto(int maxSize) const
{
    NProto::TKeyPart keyPart;
    keyPart.set_type(Type_);

    switch (Type_) {
    case EKeyType::String:
        keyPart.set_str_value(Buffer->begin() + StrOffset, std::min(maxSize, StrLength));
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

int CompareKeyParts(const TKeyPart& lhs, const TKeyPart& rhs)
{
    if (rhs.GetType() == lhs.GetType()) {
        switch (rhs.GetType()) {
        case EKeyType::String:
            return lhs.GetString().compare(rhs.GetString());

        case EKeyType::Integer:
            return lhs.GetInteger() - rhs.GetInteger();

        case EKeyType::Double:
            if (lhs.GetDouble() > rhs.GetDouble())
                return 1;
            if (lhs.GetDouble() < rhs.GetDouble())
                return -1;
            return 0;

        default:
            return 0; // All composites are equal to each other.
        }
    } else 
        return int(lhs.GetType()) - int(rhs.GetType());
}

////////////////////////////////////////////////////////////////////////////////

TKey::TKey(int columnCount, int size)
    : MaxSize(size)
    , ColumnCount(columnCount)
    , Buffer(new TBlobOutput(size))
    , Parts(ColumnCount)
{ }

void TKey::Reset(int columnCount)
{
    if (columnCount >= 0)
        ColumnCount = columnCount;

    Buffer->Clear();
    Parts.clear();
    Parts.resize(ColumnCount);
}

void TKey::AddComposite(int index)
{
    YASSERT(index < ColumnCount);
    int size = sizeof(EKeyType);
    Parts[index] = TKeyPart::CreateComposite();
}

void TKey::Swap(TKey& other)
{
    // May be one day these asserts will be deprecated.
    YASSERT(MaxSize == other.MaxSize);
    YASSERT(ColumnCount == other.ColumnCount);

    Parts.swap(other.Parts);
    std::swap(Buffer, other.Buffer);
}

Stroka TKey::ToString() const
{
    return JoinToString(Parts);
}

NProto::TKey TKey::ToProto() const
{
    NProto::TKey key;
    FOREACH(const auto& part, Parts) {
        int currentSize = key.ByteSize();
        if (currentSize < MaxSize) {
            *key.add_parts() = part.ToProto(MaxSize - currentSize);
        } else {
            *key.add_parts() = TKeyPart().ToProto();
        }
    }
    return key;
}

void TKey::FromProto(const NProto::TKey& protoKey)
{
    Reset(protoKey.parts_size());
    for (int i = 0; i < protoKey.parts_size(); ++i) {
        switch (protoKey.parts(i).type()) {
        case EKeyType::Composite:
            AddComposite(i);
            break;

        case EKeyType::Double:
            AddValue(i, protoKey.parts(i).double_value());
            break;

        case EKeyType::Integer:
            AddValue(i, protoKey.parts(i).int_value());
            break;

        case EKeyType::String:
            AddValue(i, protoKey.parts(i).str_value());
            break;

        //default: leave a null key.
        }
    }
}

void TKey::AddValue(int index, i64 value)
{
    YASSERT(index < ColumnCount);
    Parts[index] = TKeyPart(value);
}

void TKey::AddValue(int index, double value)
{
    YASSERT(index < ColumnCount);
    Parts[index] = TKeyPart(value);
}

void TKey::AddValue(int index, const TStringBuf& value)
{
    YASSERT(index < ColumnCount);

    // Strip long values.
    int length = std::min(MaxSize - sizeof(EKeyType), value.size());

    auto offset = Buffer->GetSize();
    Buffer->Write(value.begin(), length);

    Parts[index] = TKeyPart(offset, length, Buffer->GetBlob());
}

int TKey::Compare(const TKey& lhs, const TKey& rhs)
{
    int minSize = std::min(lhs.Parts.size(), rhs.Parts.size());
    for (int i = 0; i < minSize; ++i) {
        int res = CompareKeyParts(lhs.Parts[i], rhs.Parts[i]);
        if (res != 0)
            return res;
    }

    return static_cast<int>(lhs.Parts.size()) - static_cast<int>(rhs.Parts.size());
}

////////////////////////////////////////////////////////////////////////////////

int CompareProtoParts(const NProto::TKeyPart& lhs, const NProto::TKeyPart& rhs)
{
    if (lhs.type() == rhs.type()) {
        if (lhs.has_double_value()) {
            if (lhs.double_value() > rhs.double_value())
                return 1;
            if (lhs.double_value() < rhs.double_value())
                return -1;
            return 0;
        }

        if (lhs.has_int_value())
            return lhs.int_value() - rhs.int_value();

        if (lhs.has_str_value())
            return lhs.str_value().compare(rhs.str_value());

        return 0;
    } else 
        return lhs.type() - rhs.type();
}

int CompareProtoKeys(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    int minSize = std::min(lhs.parts_size(), rhs.parts_size());
    for (int i = 0; i < minSize; ++i) {
        int res = CompareProtoParts(lhs.parts(i), rhs.parts(i));
        if (res != 0)
            return res;
    }

    return static_cast<int>(lhs.parts_size()) - static_cast<int>(rhs.parts_size());
}

bool operator>(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareProtoKeys(lhs, rhs) > 0;
}

bool operator>=(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareProtoKeys(lhs, rhs) >= 0;
}

bool operator<(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareProtoKeys(lhs, rhs) < 0;
}

bool operator<=(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareProtoKeys(lhs, rhs) <= 0;
}

bool operator==(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareProtoKeys(lhs, rhs) == 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
