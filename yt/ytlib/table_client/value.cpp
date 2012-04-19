#include "stdafx.h"
#include "value.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TValue::TValue(const TStringBuf& data)
    : Data(const_cast<char*>(data.data()), data.length())
{ }

TValue::TValue()
    : Data(NULL, 0)
{ }

bool TValue::IsNull() const
{
    return !Data.Begin();
}

TStringBuf TValue::ToStringBuf() const
{
    YASSERT(!IsNull());
    return TStringBuf(Data.Begin(), Data.End());
}

int TValue::Save(TOutputStream* out)
{
    YASSERT(out);

    if (IsNull()) {
        return WriteVarUInt64(out, 0);
    } else {
        int bytesWritten = WriteVarUInt64(out, Data.Size() + 1);
        bytesWritten += static_cast<int>(Data.Size());
        out->Write(Data.Begin(), Data.Size());
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
    TStringBuf tmp(const_cast<char*>(input->Buf()), static_cast<size_t>(size));
    input->Skip(static_cast<size_t>(size));
    return TValue(tmp);
}

////////////////////////////////////////////////////////////////////////////////

int CompareKeys(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    int lhsSize = lhs.values_size();
    int rhsSize = rhs.values_size();
    int minSize = std::min(lhsSize, rhsSize);
    for (int i = 0; i < minSize; ++i) {
        const auto& lhsValue = lhs.values(i);
        const auto& rhsValue = rhs.values(i);
        int result = Stroka::compare(lhsValue, rhsValue);
        if (result != 0) {
            return result;
        }
    } 
    return lhsSize - rhsSize;
}

bool operator == (const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) == 0;
}

bool operator != (const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) != 0;
}

bool operator <  (const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) < 0;
}

////////////////////////////////////////////////////////////////////////////////

int CompareKeys(const TKey& lhs, const TKey& rhs)
{
    int lhsSize = static_cast<int>(lhs.size());
    int rhsSize = static_cast<int>(rhs.size());
    int minSize = std::min(lhsSize, rhsSize);
    for (int i = 0; i < minSize; ++i) {
        const auto& lhsValue = lhs[i];
        const auto& rhsValue = rhs[i];
        int result = Stroka::compare(lhsValue, rhsValue);
        if (result != 0) {
            return result;
        }
    }
    return lhsSize - rhsSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
