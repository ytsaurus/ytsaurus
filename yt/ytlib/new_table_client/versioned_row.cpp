#include "stdafx.h"
#include "versioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

int GetByteSize(const TVersionedValue& value)
{
    return GetByteSize(static_cast<TUnversionedValue>(value)) + MaxVarInt64Size;
}

int WriteValue(char* output, const TVersionedValue& value)
{
    int result = WriteValue(output, static_cast<TUnversionedValue>(value));
    result += WriteVarUInt64(output + result, value.Timestamp);
    return result;
}

int ReadValue(const char* input, TVersionedValue* value)
{
    int result = ReadValue(input, static_cast<TUnversionedValue*>(value));
    result += ReadVarUInt64(input + result, &value->Timestamp);
    return result;
}

size_t GetVersionedRowDataSize(int keyCount, int valueCount, int timestampCount)
{
    return sizeof(TVersionedRowHeader) +
        sizeof(TVersionedValue) * valueCount +
        sizeof(TUnversionedValue) * keyCount +
        sizeof(TTimestamp) * timestampCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
