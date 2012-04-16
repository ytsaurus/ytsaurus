#pragma once

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/misc/common.h>
#include <ytlib/misc/ref.h>
#include <ytlib/misc/nullable.h>

#include <util/stream/mem.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Array of bytes.
class TValue
{
public:
    TValue();
    //TValue(TRef data);
    TValue(const TStringBuf& data);
    bool IsNull() const;

    int Save(TOutputStream* out);
    static TValue Load(TMemoryInput* in);

    TStringBuf ToStringBuf() const;

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
