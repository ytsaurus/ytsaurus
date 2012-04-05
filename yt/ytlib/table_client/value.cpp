#include "stdafx.h"
#include "value.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TValue::TValue(TRef data)
    : Data(data)
{ }

TValue::TValue(const TStringBuf& data)
    : Data(const_cast<char*>(data.data()), data.length())
{ }

TValue::TValue()
    : Data(NULL, 0)
{ }

const char* TValue::GetData() const
{
    return Data.Begin();
}

size_t TValue::GetSize() const
{
    return Data.Size();
}

const char* TValue::Begin() const
{
    return Data.Begin();
}

const char* TValue::End() const
{
    return Data.End();
}

bool TValue::IsEmpty() const
{
    return (Data.Begin() && Data.Size() == 0);
}

bool TValue::IsNull() const
{
    return !Data.Begin();
}

Stroka TValue::ToString() const
{
    YASSERT(!IsNull());
    return Stroka(Data.Begin(), Data.End());
}

TBlob TValue::ToBlob() const
{
    return Data.ToBlob();
}

int TValue::Save(TOutputStream* out)
{
    YASSERT(out);

    if (IsNull()) {
        return WriteVarUInt64(out, 0);
    } else {
        int bytesWritten = WriteVarUInt64(out, GetSize() + 1);
        bytesWritten += static_cast<int>(GetSize());
        out->Write(GetData(), GetSize());
        return bytesWritten;
    }
}

TValue TValue::Load(TMemoryInput* input)
{
    YASSERT(input);

    ui64 size;
    ReadVarUInt64(input, &size);
    if (size == 0) {
        return TValue();
    }

    --size;
    TRef ref(const_cast<char*>(input->Buf()), static_cast<size_t>(size));
    input->Skip(static_cast<size_t>(size));
    return TValue(ref);
}

////////////////////////////////////////////////////////////////////////////////

int CompareValues(const TValue& lhs, const TValue& rhs)
{
    if (lhs.IsNull() && rhs.IsNull()) {
        return 0;
    }

    if (rhs.IsNull()) {
        return -1;
    }

    if (lhs.IsNull()) {
        return 1;
    }

    size_t lhsSize = lhs.GetSize();
    size_t rhsSize = rhs.GetSize();
    size_t minSize = Min(lhsSize, rhsSize);

    if (minSize > 0) {
        int result = memcmp(lhs.GetData(), rhs.GetData(), minSize);
        if (result != 0)
            return result;
    }

    return (int)lhsSize - (int)rhsSize;
}

bool operator==(const TValue& lhs, const TValue& rhs)
{
    return CompareValues(lhs, rhs) == 0;
}

bool operator!=(const TValue& lhs, const TValue& rhs)
{
    return CompareValues(lhs, rhs) != 0;
}

bool operator<(const TValue& lhs, const TValue& rhs)
{
    return CompareValues(lhs, rhs) < 0;
}

bool operator>(const TValue& lhs, const TValue& rhs)
{
    return CompareValues(lhs, rhs) > 0;
}

bool operator<=(const TValue& lhs, const TValue& rhs)
{
    return CompareValues(lhs, rhs) <= 0;
}

bool operator>=(const TValue& lhs, const TValue& rhs)
{
    return CompareValues(lhs, rhs) >= 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
