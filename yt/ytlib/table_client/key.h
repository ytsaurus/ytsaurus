#pragma once

#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EKeyType,
    ((None)(0))
    ((Integer)(1))
    ((Double)(2))
    ((String)(3))
    ((Composite)(4))
);

////////////////////////////////////////////////////////////////////////////////

class TKeyPart
{
public:
    TKeyPart(const TStringBuf& value);
    TKeyPart(i64 value);
    TKeyPart(double value);

    static TKeyPart CreateComposite();
    static TKeyPart CreateNone();

    EKeyType GetType() const;
    i64 GetInteger() const;
    double GetDouble() const;
    const TStringBuf& GetString() const;

    Stroka ToString() const;
    size_t GetSize() const;

    //ToProto()
    //FromProto();

private:
    TKeyPart();

    i64 IntValue;
    double DoubleValue;
    TStringBuf StrValue;
    EKeyType Type;
};

////////////////////////////////////////////////////////////////////////////////

class TKey
{
public:
    //! Creates empty key.
    TKey();
    TKey(int size);

    void AddInteger(int index, i64 value);
    void AddDouble(int index, double value);
    void AddString(int index, const TStringBuf& value);
    void AddComposite(int index);

    void Reset();
    void Swap(TKey& other);

    Stroka ToString() const;

    // ToProto
    // FromProto

    bool operator<(const TKey& other) const;

private:
    TBlobOutput Buffer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

