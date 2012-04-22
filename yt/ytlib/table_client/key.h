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

class TKeyPart
{
    DEFINE_BYVAL_RO_PROPERTY(EKeyType, Type);

public:
    // TODO(babenko): create all types with CreateXXX
    //! Creates null key part.
    TKeyPart();
    TKeyPart(const TStringBuf& value);
    TKeyPart(i64 value);
    TKeyPart(double value);

    static TKeyPart CreateComposite();

    i64 GetInteger() const;
    double GetDouble() const;
    const TStringBuf& GetString() const;

    Stroka ToString() const;

    NProto::TKeyPart ToProto() const;
    //FromProto();

private:
    // Keeps the actual value. 
    i64 IntValue;
    double DoubleValue;

    // Points to the internal buffer inside key (see |TKey::Buffer|).
    TStringBuf StrValue;
};

////////////////////////////////////////////////////////////////////////////////

class TKey
    : public TNonCopyable
{
public:
    //! Creates an empty key.
    TKey(int columnCount = 0, int maxSize = 4096);

    template <class T>
    void AddValue(int index, const T& value);
    void AddComposite(int index);

    void Reset(int columnCount = -1);
    void Swap(TKey& other);

    Stroka ToString() const;

    NProto::TKey ToProto() const;
    void FromProto(const NProto::TKey& protoKey);

    // TODO(babenko): to free function
    static int Compare(const TKey& lhs, const TKey& rhs);

private:
    const int MaxSize;
    int ColumnCount;

    int CurrentSize;
    std::vector<TKeyPart> Parts;
    TBlobOutput Buffer;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): -> CompareKeys
int CompareProtoKeys(const NProto::TKey& lhs, const NProto::TKey& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

