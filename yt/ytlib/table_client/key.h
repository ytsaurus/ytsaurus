#pragma once

#include "public.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/blob_range.h>
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

class TKeyPart
{
    DEFINE_BYVAL_RO_PROPERTY(EKeyType, Type);

public:
    // TODO(babenko): create all types with CreateXXX
    //! Creates null key part.
    TKeyPart();
    TKeyPart(const TBlobRange& value);
    TKeyPart(i64 value);
    TKeyPart(double value);

    static TKeyPart CreateComposite();

    i64 GetInteger() const;
    double GetDouble() const;
    TStringBuf GetString() const;

    size_t GetSize() const;

    Stroka ToString() const;

    //! Converts the part into Protobuf.
    //! Trims string part length to #maxSize if it exceeds the limit.
    NProto::TKeyPart ToProto(size_t maxSize = 0) const;

private:
    // The actual value. 
    i64 IntValue;
    double DoubleValue;

    // Pointer to an internal buffer inside the key.
    TBlobRange StrValue;

};

////////////////////////////////////////////////////////////////////////////////

int CompareKeys(const NProto::TKey& lhs, const NProto::TKey& rhs);
int CompareKeys(const TKey& lhs, const TKey& rhs);

bool operator >  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator >= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <  (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator <= (const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator == (const NProto::TKey& lhs, const NProto::TKey& rhs);

////////////////////////////////////////////////////////////////////////////////

class TKey
    : public TNonCopyable
{
public:
    //! Creates empty key.
    /* 
     *  \param maxSize maximum string key size.
     */
    explicit TKey(int columnCount = 0, size_t maxSize = 4096);

    void SetValue(int index, i64 value);
    void SetValue(int index, double value);
    void SetValue(int index, const TStringBuf& value);

    void SetComposite(int index);

    void Reset(int columnCount = -1);
    void Swap(TKey& other);

    size_t GetSize() const;

    // TODO(babenko): ToYson?
    Stroka ToString() const;

    NProto::TKey ToProto() const;
    void FromProto(const NProto::TKey& protoKey);

private:
    friend int CompareKeys(const TKey& lhs, const TKey& rhs);

    const size_t MaxSize;
    int ColumnCount;

    std::vector<TKeyPart> Parts;
    TAutoPtr<TBlobOutput> Buffer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

