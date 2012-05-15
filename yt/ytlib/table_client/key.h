#pragma once

#include "public.h"
#include "size_limits.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/ytree/lexer.h>
#include <ytlib/misc/enum.h>
#include <ytlib/misc/property.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EKeyType,
    ((Null)(0))
    ((Integer)(1))
    ((Double)(2))
    ((String)(3))
    ((Composite)(4))
);

////////////////////////////////////////////////////////////////////////////////

template <class TStrType>
class TKeyPart
{
    DEFINE_BYVAL_RO_PROPERTY(EKeyType, Type);

public:
    TKeyPart()
        : Type_(EKeyType::Null)
    { }

    static TKeyPart CreateNull()
    {
        TKeyPart result;
        result.Type_ = EKeyType::Null;
        return result;
    }

    static TKeyPart CreateComposite()
    {
        TKeyPart result;
        result.Type_ = EKeyType::Composite;
        return result;
    }

    static TKeyPart CreateValue(const TStrType& value)
    {
        TKeyPart result;
        result.Type_ = EKeyType::String;
        result.StrValue = value;
        return result;
    }

    static TKeyPart CreateValue(i64 value)
    {
        TKeyPart result;
        result.Type_ = EKeyType::Integer;
        result.IntValue = value;
        return result;
    }

    static TKeyPart CreateValue(double value)
    {
        TKeyPart result;
        result.Type_ = EKeyType::Double;
        result.DoubleValue = value;
        return result;
    }

    i64 GetInteger() const
    {
        YASSERT(Type_ == EKeyType::Integer);
        return IntValue;
    }

    double GetDouble() const
    {
        YASSERT(Type_ == EKeyType::Double);
        return DoubleValue;
    }

    TStringBuf GetString() const
    {
        YASSERT(Type_ == EKeyType::String);
        return TStringBuf(StrValue.begin(), StrValue.size());
    }

    size_t GetSize() const
    {
        auto typeSize = sizeof(Type_);

        switch (Type_) {
        case EKeyType::String:
            return typeSize + StrValue.size();
        case EKeyType::Integer:
            return typeSize + sizeof(i64);
        case EKeyType::Double:
            return typeSize + sizeof(double);
        default:
            return typeSize;
        }
    }

    //! Converts the part into protobuf.
    //! Trims string part length to #maxSize if it exceeds the limit.
    NProto::TKeyPart ToProto(size_t maxSize = 0) const
    {
        NProto::TKeyPart keyPart;
        keyPart.set_type(Type_);

        switch (Type_) {
        case EKeyType::String:
            keyPart.set_str_value(StrValue.begin(), StrValue.size());
            break;

        case EKeyType::Integer:
            keyPart.set_int_value(IntValue);
            break;

        case EKeyType::Double:
            keyPart.set_double_value(DoubleValue);
            break;

        case EKeyType::Null:
        case EKeyType::Composite:
            break;

        default:
            YUNREACHABLE();
        }

        return keyPart;
    }

private:
    // The actual value. 
    i64 IntValue;
    double DoubleValue;
    TStrType StrValue;

};

template <class TStrType>
Stroka ToString(const TKeyPart<TStrType>& keyPart)
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

////////////////////////////////////////////////////////////////////////////////

int CompareKeys(const NProto::TKey& lhs, const NProto::TKey& rhs);

bool operator >  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator >= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator == (const NProto::TKey& lhs, const NProto::TKey& rhs);

////////////////////////////////////////////////////////////////////////////////

template <class TBuffer>
class TKey
{
public:
    //! Creates empty key.
    /* 
     *  \param maxSize maximum string key size.
     */
    explicit TKey(int columnCount = 0)
        : ColumnCount(columnCount)
        , Buffer(columnCount * MaxKeySize)
        , Parts(ColumnCount)
    { }

    template<class TOtherBuffer>
    void operator=(const TKey<TOtherBuffer>& other)
    {
        Reset(other.Parts.size());
        for (int i = 0; i < other.Parts.size(); ++i) {
            switch (other.Parts[i].GetType()) {
            case EKeyType::Composite:
                SetComposite(i);
                break;

            case EKeyType::Integer:
                SetValue(i, other.Parts[i].GetInteger());
                break;

            case EKeyType::Double:
                SetValue(i, other.Parts[i].GetDouble());
                break;

            case EKeyType::String:
                SetValue(i, other.Parts[i].GetString());
                break;

            case EKeyType::Null:
                // Do nothing.
                break;

            default:
                YUNREACHABLE();
            }
        }
    }

    void SetValue(int index, i64 value)
    {
        YASSERT(index < ColumnCount);
        Parts[index] = TKeyPart<typename TBuffer::TStoredType>::CreateValue(value);
    }

    void SetValue(int index, double value)
    {
        YASSERT(index < ColumnCount);
        Parts[index] = TKeyPart<typename TBuffer::TStoredType>::CreateValue(value);
    }

    void SetValue(int index, const TStringBuf& value)
    {
        YASSERT(index < ColumnCount);

        // Strip long values.
        int length = std::min(MaxKeySize - sizeof(EKeyType), value.size());

        auto part = Buffer.PutData(TStringBuf(value.begin(), length));

        Parts[index] = TKeyPart<typename TBuffer::TStoredType>::CreateValue(part);
    }

    void SetComposite(int index)
    {
        YASSERT(index < ColumnCount);
        Parts[index] = TKeyPart<typename TBuffer::TStoredType>::CreateComposite();
    }

    void Reset(int columnCount = -1)
    {
        if (columnCount >= 0)
            ColumnCount = columnCount;

        Buffer.Clear();
        Parts.clear();
        Parts.resize(ColumnCount);
    }

    size_t GetSize() const
    {
        size_t result = 0;
        FOREACH (const auto& part, Parts) {
            result += part.GetSize();
        }
        return std::min(result, MaxKeySize);
    }

    Stroka ToString() const
    {
        return JoinToString(Parts);
    }

    NProto::TKey ToProto() const
    {
        NProto::TKey key;
        size_t currentSize = 0;
        FOREACH (const auto& part, Parts) {
            if (currentSize < MaxKeySize) {
                *key.add_parts() = part.ToProto(MaxKeySize - currentSize);
                currentSize += part.GetSize();
            } else {
                *key.add_parts() = TKeyPart<typename TBuffer::TStoredType>::CreateNull().ToProto();
            }
        }
        return key;
    }

    void FromProto(const NProto::TKey& protoKey)
    {
        Reset(protoKey.parts_size());
        for (int i = 0; i < protoKey.parts_size(); ++i) {
            switch (protoKey.parts(i).type()) {
            case EKeyType::Composite:
                SetComposite(i);
                break;

            case EKeyType::Double:
                SetValue(i, protoKey.parts(i).double_value());
                break;

            case EKeyType::Integer:
                SetValue(i, protoKey.parts(i).int_value());
                break;

            case EKeyType::String:
                SetValue(i, protoKey.parts(i).str_value());
                break;

            case EKeyType::Null:
                break;

            default:
                YUNREACHABLE();
            }
        }
    }

    void SetKeyPart(int index, const TStringBuf& yson, NYTree::TLexer& lexer)
    {
        lexer.Reset();
        YVERIFY(lexer.Read(yson) > 0);
        YASSERT(lexer.GetState() == NYTree::TLexer::EState::Terminal);

        const auto& token = lexer.GetToken();
        switch (token.GetType()) {
            case NYTree::ETokenType::Integer:
                SetValue(index, token.GetIntegerValue());
                break;

            case NYTree::ETokenType::String:
                SetValue(index, token.GetStringValue());
                break;

            case NYTree::ETokenType::Double:
                SetValue(index, token.GetDoubleValue());
                break;

            default:
                SetComposite(index);
                break;
        }
    }

    // This is required for correct compilation of operator =.
    template <class TOtherBuffer>
    friend class TKey;

private:
    template<class TLhsBuffer, class TRhsBuffer>
    friend int CompareKeys(const TKey<TLhsBuffer>& lhs, const TKey<TRhsBuffer>& rhs);

    int ColumnCount;

    std::vector< TKeyPart<typename TBuffer::TStoredType> > Parts;
    TBuffer Buffer;
};

////////////////////////////////////////////////////////////////////////////////

template<class TLhsStrType, class TRhsStrType>
int CompareKeyParts(const TKeyPart<TLhsStrType>& lhs, const TKeyPart<TRhsStrType>& rhs)
{
    if (rhs.GetType() != lhs.GetType()) {
        return static_cast<int>(lhs.GetType()) - static_cast<int>(rhs.GetType());
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

        case EKeyType::Null:
        case EKeyType::Composite:
            return 0; // All composites are equal to each other.

        default:
            YUNREACHABLE();
    }
}

template<class TLhsBuffer, class TRhsBuffer>
int CompareKeys(const TKey<TLhsBuffer>& lhs, const TKey<TRhsBuffer>& rhs)
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

} // namespace NTableClient
} // namespace NYT

