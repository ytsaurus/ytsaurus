#pragma once

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <core/misc/public.h>
#include <core/misc/ref.h>
#include <core/misc/nullable.h>

#include <util/stream/mem.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Array of bytes.
class TValue
{
public:
    TValue();
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
