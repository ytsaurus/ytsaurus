#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Array of bytes. The only data type stored inside tables (column names and values)
class TValue
{
public:
    TValue();
    TValue(TRef data);
    TValue(Stroka data);

    const char* GetData() const;
    size_t GetSize() const;

    const char* Begin() const;
    const char* End() const;

    bool IsEmpty() const;
    bool IsNull() const;

    Stroka ToString() const;
    TBlob ToBlob() const;

    static TValue Null();

private:
    TRef Data;
};

bool operator==(const TValue& lhs, const TValue& rhs);
bool operator!=(const TValue& lhs, const TValue& rhs);
bool operator< (const TValue& lhs, const TValue& rhs);
bool operator> (const TValue& lhs, const TValue& rhs);
bool operator<=(const TValue& lhs, const TValue& rhs);
bool operator>=(const TValue& lhs, const TValue& rhs);

// construct from POD types
template <class T>
TValue Value(const T& data)
{
    TBlob blob(&data, &data + sizeof(T));
    return TValue(blob);
}

template <>
inline TValue Value<Stroka>(const Stroka& data)
{
    TBlob blob(~data, ~data + data.Size());
    return TValue(blob);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////
