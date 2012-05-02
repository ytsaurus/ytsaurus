#pragma once

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/misc/blob_output.h>
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

class TKey;

class TKeyPart
{
    DEFINE_BYVAL_RO_PROPERTY(EKeyType, Type);

public:
    //! Creates null key part.
    TKeyPart();
    TKeyPart(size_t offset, int length, const TBlob* buffer);
    TKeyPart(i64 value);
    TKeyPart(double value);

    static TKeyPart CreateComposite();

    i64 GetInteger() const;
    double GetDouble() const;
    TStringBuf GetString() const;

    Stroka ToString() const;

    // Makes protobuf representation. Strips string part length to maxSize if exceeds.
    NProto::TKeyPart ToProto(int maxSize = 0) const;
    //FromProto();

private:
    i64 IntValue;
    double DoubleValue;

    // Offset in the internal buffer of TKey.
    size_t StrOffset;
    int StrLength;
    const TBlob* Buffer;
};

////////////////////////////////////////////////////////////////////////////////

class TKey
    : public TNonCopyable
{
public:
    //! Creates empty key.
    /* 
     *  \param size - maximum key size.
     */
    TKey(int columnCount = 0, int size = 4096);

    void AddValue(int index, i64 value);
    void AddValue(int index, double value);
    void AddValue(int index, const TStringBuf& value);

    void AddComposite(int index);

    void Reset(int columnCount = -1);
    void Swap(TKey& other);

    Stroka ToString() const;

    NProto::TKey ToProto() const;
    void FromProto(const NProto::TKey& protoKey);

    static int Compare(const TKey& lhs, const TKey& rhs);

private:
    friend class TKey;

    const int MaxSize;
    int ColumnCount;

    std::vector<TKeyPart> Parts;
    TAutoPtr<TBlobOutput> Buffer;
};

////////////////////////////////////////////////////////////////////////////////

int CompareProtoKeys(const NProto::TKey& lhs, const NProto::TKey& rhs);

bool operator>(const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator>=(const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator<(const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator<=(const NProto::TKey& lhs, const NProto::TKey& rhs);
bool operator==(const NProto::TKey& lhs, const NProto::TKey& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

