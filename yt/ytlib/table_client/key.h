#pragma once

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

class TKey;

class TKeyPart
{
    DEFINE_BYVAL_RO_PROPERTY(EKeyType, Type);

public:
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

    // Makes protobuf representation. Strips string part length to maxSize if exceeds.
    NProto::TKeyPart ToProto(size_t maxSize = 0) const;
    //FromProto();

private:
    i64 IntValue;
    double DoubleValue;
    TBlobRange StrValue;
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
    TKey(int columnCount = 0, size_t size = 4096);

    void AddValue(int index, i64 value);
    void AddValue(int index, double value);
    void AddValue(int index, const TStringBuf& value);

    void AddComposite(int index);

    void Reset(int columnCount = -1);
    void Swap(TKey& other);

    size_t GetSize() const;

    Stroka ToString() const;

    NProto::TKey ToProto() const;
    void FromProto(const NProto::TKey& protoKey);

    static int Compare(const TKey& lhs, const TKey& rhs);

private:
    const size_t MaxSize;
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

