#include "stdafx.h"
#include "key.h"

#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TKeyPart::TKeyPart()
    : Type_(EKeyType::Null)
{ }

TKeyPart::TKeyPart(const TBlobRange& range)
    : StrValue(range)
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
    return StrValue.GetStringBuf();
}

Stroka ToString(const TKeyPart& keyPart)
{
    switch (keyPart.GetType()) {
        case EKeyType::Null:
            return "<null>";
        case EKeyType::Composite:
            return "<composite>";
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

size_t TKeyPart::GetSize() const
{
    auto typeSize = sizeof(Type_);

    switch (Type_) {
        case EKeyType::String:
            return typeSize + StrValue.GetStringBuf().size();
        case EKeyType::Integer:
            return typeSize + sizeof(i64);
        case EKeyType::Double:
            return typeSize + sizeof(double);
        default:
            return typeSize;
    }
}

NProto::TKeyPart TKeyPart::ToProto(size_t maxSize) const
{
    NProto::TKeyPart keyPart;
    keyPart.set_type(Type_);

    switch (Type_) {
	    case EKeyType::String:
            keyPart.set_str_value(StrValue.GetStringBuf().begin(), StrValue.GetStringBuf().size());
            break;

        case EKeyType::Integer:
            keyPart.set_int_value(IntValue);
            break;

        case EKeyType::Double:
            keyPart.set_double_value(DoubleValue);
            break;

        case EKeyType::Null:
            break;

        default:
            YUNREACHABLE();
    }

    return keyPart;
}

int CompareKeyParts(const TKeyPart& lhs, const TKeyPart& rhs)
{
    if (rhs.GetType() != lhs.GetType()) {
        return static_cast<int>(lhs.GetType()) -
               static_cast<int>(rhs.GetType());
    }

    switch (rhs.GetType()) {
        case EKeyType::String:
            return lhs.GetString().compare(rhs.GetString());

        case EKeyType::Integer:
            if (lhs.GetInteger() > rhs.GetInteger())
                return 1;
            if (lhs.GetInteger() < rhs.GetInteger())
                return -1;
            return 0;

        case EKeyType::Double:
            if (lhs.GetDouble() > rhs.GetDouble())
                 return 1;
            if (lhs.GetDouble() < rhs.GetDouble())
                return -1;
            return 0;

        case EKeyType::Composite:
            return 0; // All composites are equal to each other.

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

TKey::TKey(int columnCount, size_t size)
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

size_t TKey::GetSize() const 
{
    size_t result = 0;
    FOREACH(const auto part, Parts) {
        result += part.GetSize();
    }
    return std::min(result, MaxSize);
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
    return "";//return JoinToString(Parts);
}

NProto::TKey TKey::ToProto() const
{
    NProto::TKey key;
    size_t currentSize = 0;
    FOREACH(const auto& part, Parts) {
        if (currentSize < MaxSize) {
            *key.add_parts() = part.ToProto(MaxSize - currentSize);
            currentSize += part.GetSize();
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

            case EKeyType::Null:
                break;

            default:
                YUNREACHABLE();
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

    Parts[index] = TKeyPart(TBlobRange(Buffer->GetBlob(), offset, length));
}

int TKey::Compare(const TKey& lhs, const TKey& rhs)
{
    int minSize = std::min(lhs.Parts.size(), rhs.Parts.size());
    for (int i = 0; i < minSize; ++i) {
        int result = CompareKeyParts(lhs.Parts[i], rhs.Parts[i]);
        if (result != 0) {
            return result;
        }
    }
    return static_cast<int>(lhs.Parts.size()) - static_cast<int>(rhs.Parts.size());
}

////////////////////////////////////////////////////////////////////////////////

int CompareProtoParts(const NProto::TKeyPart& lhs, const NProto::TKeyPart& rhs)
{
    if (lhs.type() != rhs.type()) {
        return lhs.type() - rhs.type();
    }

    if (lhs.has_double_value()) {
        if (lhs.double_value() > rhs.double_value())
            return 1;
        if (lhs.double_value() < rhs.double_value())
            return -1;
        return 0;
    }

    if (lhs.has_int_value()) {
        if (lhs.int_value() > rhs.int_value())
            return 1;
        if (lhs.int_value() < rhs.int_value())
            return -1;
        return 0;
    }

    if (lhs.has_str_value()) {
        return lhs.str_value().compare(rhs.str_value());
    }

    return 0;
}

int CompareProtoKeys(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    int minSize = std::min(lhs.parts_size(), rhs.parts_size());
    for (int i = 0; i < minSize; ++i) {
        int result = CompareProtoParts(lhs.parts(i), rhs.parts(i));
        if (result != 0) {
            return result;
        }
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
