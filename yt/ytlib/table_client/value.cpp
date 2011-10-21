#include "stdafx.h"
#include "value.h"

#include "../misc/serialize.h"
#include "../misc/assert.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TValue::TValue(TRef data)
    : Data(data)
{ }

TValue::TValue(const Stroka& data)
    : Data(const_cast<char*>(data.begin()), data.Size())
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
    return (Data.Begin() != NULL) && (Data.Size() == 0);
}

bool TValue::IsNull() const
{
    return Data.Begin() == NULL;
}

Stroka TValue::ToString() const
{
    return Stroka(Data.Begin(), Data.End());
}

TBlob TValue::ToBlob() const
{
    return Data.ToBlob();
}

int TValue::Save(TOutputStream* out)
{
    if (IsNull()) {
        return WriteVarInt(0, out);
    } else {
        int bytesWritten = WriteVarInt(GetSize() + 1, out);
        bytesWritten += GetSize();
        out->Write(GetData(), GetSize());
        return bytesWritten;
    }
}

TValue TValue::Load(TMemoryInput* input)
{
    ui64 size;
    ReadVarInt(&size, input);
    if (size == 0) {
        return TValue();
    } else {
        --size;
        auto ref = TRef(const_cast<char*>(input->Buf()), size);
        input->Skip(size);
        return TValue(ref);
    }
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

int CompareValue(TValue lhs, TValue rhs)
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
    return CompareValue(lhs, rhs) == 0;
}

bool operator!=(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) == 0;
}

bool operator<(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) < 0;
}

bool operator>(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) > 0;
}

bool operator<=(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) <= 0;
}

bool operator>=(const TValue& lhs, const TValue& rhs)
{
    return CompareValue(lhs, rhs) >= 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
