#pragma once

#include "public.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/misc/enum.h>
#include <ytlib/misc/property.h>

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
    static TKeyPart CreateNull();
    static TKeyPart CreateComposite();
    static TKeyPart CreateValue(const TStrType& value);
    static TKeyPart CreateValue(i64 value);
    static TKeyPart CreateValue(double value);

    i64 GetInteger() const;
    double GetDouble() const;
    TStringBuf GetString() const;

    size_t GetSize() const;

    Stroka ToString() const;

    //! Converts the part into protobuf.
    //! Trims string part length to #maxSize if it exceeds the limit.
    NProto::TKeyPart ToProto(size_t maxSize = 0) const;

private:
    // The actual value. 
    i64 IntValue;
    double DoubleValue;
    TStrType StrValue;

};

////////////////////////////////////////////////////////////////////////////////

int CompareKeys(const NProto::TKey& lhs, const NProto::TKey& rhs);

template<class TLhsBuffer, class TRhsBuffer>
int CompareKeys(const TKey<TLhsBuffer>& lhs, const TKey<TRhsBuffer>& rhs);

bool operator >  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator >= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator == (const NProto::TKey& lhs, const NProto::TKey& rhs);

////////////////////////////////////////////////////////////////////////////////

template <class TBuffer>
class TKey
    : public TNonCopyable
{
public:
    //! Creates empty key.
    /* 
     *  \param maxSize maximum string key size.
     */
    explicit TKey(int columnCount = 0);

    void SetValue(int index, i64 value);
    void SetValue(int index, double value);
    void SetValue(int index, const TStringBuf& value);

    void SetComposite(int index);

    void Reset(int columnCount = -1);

    size_t GetSize() const;

    // TODO(babenko): ToYson?
    Stroka ToString() const;

    NProto::TKey ToProto() const;
    void FromProto(const NProto::TKey& protoKey);

private:
    template<class TLhsBuffer, class TRhsBuffer>
    friend int CompareKeys(const TKey<TLhsBuffer>& lhs, const TKey<TRhsBuffer>& rhs);

    int ColumnCount;

    std::vector< TKeyPart<TBuffer::TStrType> > Parts;
    TBuffer Buffer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

