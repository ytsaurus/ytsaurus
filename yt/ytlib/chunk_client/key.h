#pragma once

#include "public.h"

#include <core/misc/small_vector.h>
#include <core/misc/property.h>
#include <core/misc/string.h>
#include <core/misc/nullable.h>
#include <core/misc/blob_output.h>

#include <core/yson/lexer.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EKeyPartType,
    // A special sentinel used by #GetKeySuccessor.
    ((MinSentinel)(-1))
    // Denotes a missing (null) component in a composite key.
    ((Null)(0))
    // Integer value.
    ((Integer)(1))
    // Floating-point value.
    ((Double)(2))
    // String value.
    ((String)(3))
    // Any structured value.
    ((Composite)(4))

    // A special sentinel used by #GetKeyPrefixSuccessor.
    ((MaxSentinel)(100))
);

////////////////////////////////////////////////////////////////////////////////

template <class TStrType>
class TKeyPart
{
    DEFINE_BYVAL_RO_PROPERTY(EKeyPartType, Type);

public:
    TKeyPart()
        : Type_(EKeyPartType::Null)
    { }

    static TKeyPart CreateSentinel(EKeyPartType type)
    {
        TKeyPart result;
        result.Type_ = type;
        return result;
    }

    static TKeyPart CreateValue(const TStrType& value)
    {
        TKeyPart result;
        result.Type_ = EKeyPartType::String;
        result.StringValue = value;
        return result;
    }

    static TKeyPart CreateValue(i64 value)
    {
        TKeyPart result;
        result.Type_ = EKeyPartType::Integer;
        result.IntValue = value;
        return result;
    }

    static TKeyPart CreateValue(double value)
    {
        TKeyPart result;
        result.Type_ = EKeyPartType::Double;
        result.DoubleValue = value;
        return result;
    }

    void SetSentinel(EKeyPartType type)
    {
        Type_ = type;
    }

    void SetValue(const TStrType& value)
    {
        Type_ = EKeyPartType::String;
        StringValue = value;
    }

    void SetValue(i64 value)
    {
        Type_ = EKeyPartType::Integer;
        IntValue = value;
    }

    void SetValue(double value)
    {
        Type_ = EKeyPartType::Double;
        DoubleValue = value;
    }

    i64 GetInteger() const
    {
        YASSERT(Type_ == EKeyPartType::Integer);
        return IntValue;
    }

    double GetDouble() const
    {
        YASSERT(Type_ == EKeyPartType::Double);
        return DoubleValue;
    }

    const char* Begin() const
    {
        YASSERT(Type_ == EKeyPartType::String);
        return &*StringValue.begin();
    }

    size_t GetStringSize() const
    {
        YASSERT(Type_ == EKeyPartType::String);
        return StringValue.size();
    }

    TStringBuf GetString() const
    {
        YASSERT(Type_ == EKeyPartType::String);
        return TStringBuf(&*StringValue.begin(), StringValue.size());
    }

    size_t GetSize() const
    {
        size_t result = sizeof(Type_);
        switch (Type_) {
            case EKeyPartType::String:
                result += StringValue.size();
                break;
            case EKeyPartType::Integer:
                result += sizeof(i64);
                break;
            case EKeyPartType::Double:
                result += sizeof(double);
                break;
            default:
                break;
        }
        return result;
    }

    //! Converts the part into protobuf.
    NProto::TKeyPart ToProto() const
    {
        NProto::TKeyPart keyPart;
        keyPart.set_type(Type_);

        switch (Type_) {
            case EKeyPartType::String:
                keyPart.set_str_value(&*StringValue.begin(), StringValue.size());
                break;

            case EKeyPartType::Integer:
                keyPart.set_int_value(IntValue);
                break;

            case EKeyPartType::Double:
                keyPart.set_double_value(DoubleValue);
                break;

            case EKeyPartType::MinSentinel:
            case EKeyPartType::Null:
            case EKeyPartType::Composite:
            case EKeyPartType::MaxSentinel:
                break;

            default:
                YUNREACHABLE();
        }

        return keyPart;
    }

    ui32 GetHash() const
    {
        switch (Type_) {
            case EKeyPartType::String:
                return static_cast<ui32>(StringValue.hash());
            case EKeyPartType::Integer:
            case EKeyPartType::Double:
                // Integer and Double are aliased (note "union" below).
                return static_cast<ui32>((IntValue & 0xffff) + 17 * (IntValue >> 32));
            default:
                // No idea how to hash other types.
                return 0;
        }
    }

private:
    // The actual value.
    // XXX(babenko): consider storing StringValue in the union as well.
    TStrType StringValue;
    union
    {
        i64 IntValue;
        double DoubleValue;
    };

};

////////////////////////////////////////////////////////////////////////////////

template <class TStrType>
Stroka ToString(const NYT::NChunkClient::TKeyPart<TStrType>& keyPart)
{
    switch (keyPart.GetType()) {
        case NYT::NChunkClient::EKeyPartType::Null:
            return "<Null>";
        case NYT::NChunkClient::EKeyPartType::Composite:
            return "<Composite>";
        case NYT::NChunkClient::EKeyPartType::MinSentinel:
            return "<Min>";
        case NYT::NChunkClient::EKeyPartType::MaxSentinel:
            return "<Max>";
        case NYT::NChunkClient::EKeyPartType::String:
            return keyPart.GetString().ToString().Quote();
        case NYT::NChunkClient::EKeyPartType::Integer:
            return ::ToString(keyPart.GetInteger());
        case NYT::NChunkClient::EKeyPartType::Double:
            return ::ToString(keyPart.GetDouble());
        default:
            YUNREACHABLE();
    }
}

template <class TLhsStrType, class TRhsStrType>
int CompareKeyParts(const TKeyPart<TLhsStrType>& lhs, const TKeyPart<TRhsStrType>& rhs)
{
    if (rhs.GetType() != lhs.GetType()) {
        return static_cast<int>(lhs.GetType()) - static_cast<int>(rhs.GetType());
    }

    switch (rhs.GetType()) {
        case EKeyPartType::String: {
            // Too slow because of allocations:
            // return lhs.GetString().compare(rhs.GetString());
            size_t minLen = std::min(lhs.GetStringSize(), rhs.GetStringSize());
            int result = memcmp(lhs.Begin(), rhs.Begin(), minLen);
            if (result != 0) {
                return result;
            }
            return static_cast<int>(lhs.GetStringSize()) - static_cast<int>(rhs.GetStringSize());
        }

        case EKeyPartType::Integer:
            if (lhs.GetInteger() > rhs.GetInteger())
                return 1;
            if (lhs.GetInteger() < rhs.GetInteger())
                return -1;
            return 0;

        case EKeyPartType::Double:
            if (lhs.GetDouble() > rhs.GetDouble())
                return 1;
            if (lhs.GetDouble() < rhs.GetDouble())
                return -1;
            return 0;

        case EKeyPartType::Null:
        case EKeyPartType::Composite:
        case EKeyPartType::MinSentinel:
        case EKeyPartType::MaxSentinel:
            return 0; // All sentinels are considered equal.

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TBuffer>
class TKey;

template <class TBuffer>
Stroka ToString(const NYT::NChunkClient::TKey<TBuffer>& key);

//ToDo(psushin): make key-inl.h

template <class TBuffer>
class TKey
{
public:
    explicit TKey(int columnCount = 0)
    {
        ClearAndResize(columnCount);
    }

    TKey(const TKey<TBuffer>& other)
    {
        Assign(other);
    }

    template <class TOtherBuffer>
    TKey(const TKey<TOtherBuffer>& other)
    {
        Assign(other);
    }

    template <class TOtherBuffer>
    TKey<TBuffer>& operator=(const TKey<TOtherBuffer>& other)
    {
        Assign(other);
        return *this;
    }

    TKey<TBuffer>& operator=(const TKey<TBuffer>& other)
    {
        if (this != &other) {
            Assign(other);
        }
        return *this;
    }

    void SetValue(int index, i64 value)
    {
        Parts[index].SetValue(value);
    }

    void SetValue(int index, double value)
    {
        Parts[index].SetValue(value);
    }

    void SetValue(int index, const TStringBuf& value)
    {
        auto storedValue = Buffer.PutData(TStringBuf(value.begin(), value.size()));
        Parts[index].SetValue(storedValue);
    }

    void SetSentinel(int index, EKeyPartType type)
    {
        Parts[index].SetSentinel(type);
    }

    void Clear()
    {
        auto size = Parts.size();
        Parts.clear();
        Parts.resize(size);

        Buffer.Clear();
    }

    void ClearAndResize(int columnCount)
    {
        Parts.clear();
        Parts.resize(columnCount);
        Buffer.Clear();
    }

    size_t GetSize() const
    {
        size_t result = 0;
        FOREACH (const auto& part, Parts) {
            result += part.GetSize();
        }
        return result;
    }

    NProto::TKey ToProto() const
    {
        NProto::TKey key;
        FOREACH (const auto& part, Parts) {
            *key.add_parts() = part.ToProto();
        }
        return key;
    }

    static TKey FromProto(const NProto::TKey& protoKey)
    {
        TKey key;
        key.ClearAndResize(protoKey.parts_size());
        for (int i = 0; i < protoKey.parts_size(); ++i) {
            const auto& part = protoKey.parts(i);
            auto partType = EKeyPartType(part.type());
            switch (partType) {
                case EKeyPartType::Null:
                case EKeyPartType::MinSentinel:
                case EKeyPartType::MaxSentinel:
                case EKeyPartType::Composite:
                    key.SetSentinel(i, partType);
                    break;

                case EKeyPartType::Double:
                    key.SetValue(i, part.double_value());
                    break;

                case EKeyPartType::Integer:
                    key.SetValue(i, part.int_value());
                    break;

                case EKeyPartType::String:
                    key.SetValue(i, part.str_value());
                    break;

                default:
                    YUNREACHABLE();
            }
        }
        return key;
    }

    void SetKeyPart(int index, const TStringBuf& yson, NYson::TStatelessLexer& lexer)
    {
        NYson::TToken token;
        lexer.GetToken(yson, &token);
        YCHECK(!token.IsEmpty());

        switch (token.GetType()) {
            case NYson::ETokenType::Integer:
                SetValue(index, token.GetIntegerValue());
                break;

            case NYson::ETokenType::String:
                SetValue(index, token.GetStringValue());
                break;

            case NYson::ETokenType::Double:
                SetValue(index, token.GetDoubleValue());
                break;

            default:
                SetSentinel(index, EKeyPartType::Composite);
                break;
        }
    }

    ui32 GetHash() const
    {
        ui32 result = 0xdeadc0de;
        int partCount = static_cast<int>(Parts.size());
        for (int i = 0; i < partCount; ++i) {
            result = (result * 1000003) ^ Parts[i].GetHash();
        }
        return result ^ partCount;
    }

private:
    template <class TLhsBuffer, class TRhsBuffer>
    friend int CompareKeys(const TKey<TLhsBuffer>& lhs, const TKey<TRhsBuffer>& rhs, int prefixLength);

    friend Stroka ToString<>(const TKey& key);

    // This is required for correct compilation of operator =.
    template <class TOtherBuffer>
    friend class TKey;

    //! Parts comprising the key.
    TSmallVector< TKeyPart<typename TBuffer::TStoredType>, 4> Parts;

    //! Buffer where all string parts store their data (in case of an owning key).
    TBuffer Buffer;

    template <class TOtherBuffer>
    void Assign(const TKey<TOtherBuffer>& other)
    {
        ClearAndResize(other.Parts.size());
        for (int i = 0; i < other.Parts.size(); ++i) {
            const auto& part = other.Parts[i];
            switch (part.GetType()) {
                case EKeyPartType::Composite:
                case EKeyPartType::Null:
                case EKeyPartType::MinSentinel:
                case EKeyPartType::MaxSentinel:
                    SetSentinel(i, part.GetType());
                    break;

                case EKeyPartType::Integer:
                    SetValue(i, part.GetInteger());
                    break;

                case EKeyPartType::Double:
                    SetValue(i, part.GetDouble());
                    break;

                case EKeyPartType::String:
                    SetValue(i, part.GetString());
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TBuffer>
Stroka ToString(const TKey<TBuffer>& key)
{
    return "[" + JoinToString(key.Parts) + "]";
}

//! Compares given keys (truncated to #prefixLength). Returns zero if |lhs == rhs|, a negative value
//! if |lhs < rhs| and a positive value otherwise.
template <class TLhsBuffer, class TRhsBuffer>
int CompareKeys(const TKey<TLhsBuffer>& lhs, const TKey<TRhsBuffer>& rhs, int prefixLength = std::numeric_limits<int>::max())
{
    int lhsSize = std::min(static_cast<int>(lhs.Parts.size()), prefixLength);
    int rhsSize = std::min(static_cast<int>(rhs.Parts.size()), prefixLength);
    int minSize = std::min(lhsSize, rhsSize);
    for (int index = 0; index < minSize; ++index) {
        int result = CompareKeyParts(lhs.Parts[index], rhs.Parts[index]);
        if (result != 0) {
            return result;
        }
    }
    return lhsSize - rhsSize;
}

namespace NProto {

Stroka ToString(const NProto::TKey& key);

int CompareKeys(const NProto::TKey& lhs, const NProto::TKey& rhs, int prefixLength = std::numeric_limits<int>::max());

bool operator >  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator >= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator == (const NProto::TKey& lhs, const NProto::TKey& rhs);

//! Returns the successor of |key|, i.e. the key
//! obtained from |key| by appending a <min> sentinel part.
NProto::TKey GetKeySuccessor(const NProto::TKey& key);

//! Returns the successor of |key| trimmed to given length, i.e. the key
//! obtained from trim(key) and appending a <max> sentinel part.
NProto::TKey GetKeyPrefixSuccessor(const NProto::TKey& key, int prefixLength);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

