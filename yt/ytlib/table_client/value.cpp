#include "stdafx.h"
#include "value.h"

#include <ytlib/misc/serialize.h>

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
    return (Data.Begin() && Data.Size() == 0);
}

bool TValue::IsNull() const
{
    return !Data.Begin();
}

TNullable<Stroka> TValue::ToString() const
{
    if (IsNull())
        return TNullable<Stroka>(Null);

    return TNullable<Stroka>(Data.Begin(), Data.End());
}

TBlob TValue::ToBlob() const
{
    return Data.ToBlob();
}

NProto::TValue TValue::ToProto() const
{
    NProto::TValue res;

    res.set_is_null(IsNull());
    res.set_data(Data.Begin(), Data.Size());

    return res;
}

NProto::TValue TValue::ToProto(TNullable<Stroka> strValue)
{
    res.set_is_null(strValue.IsInitialized());
    if (strValue.IsInitialized())
        res.set_data(strValue->begin(), strValue->size());
    else
        res.set_data(NULL, 0);
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
    return CompareValues(lhs, rhs) == 0;
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

bool operator==(const TKey& lhs, const TKey& rhs)
{
    YASSERT(lhs.size() == rhs.size());
    for(int i = 0; i < lhs.size(); ++i) {
        if (lhs[i] != rhs[i])
            return false;
    }
    return true;
}

bool operator!=(const TKey& lhs, const TKey& rhs)
{
    return !(lhs == rhs);
}

inline bool Less(const TKey& lhs, const TKey& rhs, bool allowEqual)
{
    YASSERT(lhs.size() == rhs.size());
    for(int i = 0; i < lhs.size(); ++i) {
        if (lhs[i] < rhs[i])
            return true;

        if (lhs[i] > rhs[i])
            return false;
    }
    return allowEqual;
}

bool operator< (const TKey& lhs, const TKey& rhs)
{
    return Less(lhs, rhs, false);
}

bool operator<=(const TKey& lhs, const TKey& rhs)
{
    return Less(lhs, rhs, true);
}

bool operator> (const TKey& lhs, const TKey& rhs)
{
    return !Less(lhs, rhs, true);
}

bool operator>=(const TKey& lhs, const TKey& rhs)
{
    return !Less(lhs, rhs, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
