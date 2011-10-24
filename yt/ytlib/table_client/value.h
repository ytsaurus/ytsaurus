#pragma once

#include "../misc/common.h"
#include "../misc/ref.h"

#include <util/stream/mem.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Array of bytes. The only data type stored inside tables (column names and values).
class TValue
{
public:
    TValue();
    TValue(TRef data);
    TValue(const Stroka& data);

    const char* GetData() const;
    size_t GetSize() const;

    const char* Begin() const;
    const char* End() const;

    bool IsEmpty() const;
    bool IsNull() const;

    int Save(TOutputStream* out);
    static TValue Load(TMemoryInput* in);

    // ToDo: do we really need it?
    Stroka ToString() const;
    TBlob ToBlob() const;

private:
    TRef Data;
};

bool operator==(const TValue& lhs, const TValue& rhs);
bool operator!=(const TValue& lhs, const TValue& rhs);
bool operator< (const TValue& lhs, const TValue& rhs);
bool operator> (const TValue& lhs, const TValue& rhs);
bool operator<=(const TValue& lhs, const TValue& rhs);
bool operator>=(const TValue& lhs, const TValue& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
