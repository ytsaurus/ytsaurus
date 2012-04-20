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

DECLARE_BYVAL_RO_PROPERTY(EKeyType, Type);

public:
    //! Created null key part.
    TKeyPart();
    TKeyPart(const TStringBuf& value);
    TKeyPart(i64 value);
    TKeyPart(double value);

    static TKeyPart CreateComposite();

    i64 GetInteger() const;
    double GetDouble() const;
    const TStringBuf& GetString() const;

    Stroka ToString() const;
    //size_t GetSize() const;

    NProto::TKeyPart ToProto() const;
    //FromProto();

private:
    i64 IntValue;
    double DoubleValue;

    // Point to the internal buffer inside key (TKey::Buffer).
    TStringBuf StrValue;
    EKeyType Type;
};

////////////////////////////////////////////////////////////////////////////////

class TKey
{
public:
    //! Creates empty key.
    /* 
     *  \param size - maximum key size.
     */
    TKey(int columnCount = 0, int size = 4096);

    template <class T>
    void AddValue(int index, const T& value);

    void AddComposite(int index);

    void Reset();
    void SetColumnCount(int columnCount);
    void Swap(TKey& other);

    Stroka ToString() const;

    NProto::TKey ToProto() const;
    // FromProto

    bool operator<(const TKey& other) const;

private:
    const int MaxSize;
    int ColumnCount;

    int CurrentSize;
    std::vector<TKeyPart> Parts;
    TBlobOutput Buffer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

