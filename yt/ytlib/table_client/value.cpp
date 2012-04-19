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

} // namespace NTableClient
} // namespace NYT
