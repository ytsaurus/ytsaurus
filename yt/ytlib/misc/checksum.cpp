#include "stdafx.h"
#include "checksum.h"

#include <util/digest/crc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

TChecksum GetChecksumImpl(const void* data, size_t length, TChecksum seed)
{
    return crc64(data, length, seed);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TChecksum GetChecksum(TRef data)
{
    return GetChecksumImpl(data.Begin(), data.Size(), 0);
}

////////////////////////////////////////////////////////////////////////////////

TChecksummableInput::TChecksummableInput(TInputStream* input)
    : Input(input)
    , Checksum(0)
{ }

TChecksum TChecksummableInput::GetChecksum() const
{
    return Checksum;
}

size_t TChecksummableInput::DoRead(void* buf, size_t len)
{
    size_t res = Input->Read(buf, len);
    Checksum = GetChecksumImpl(buf, res, Checksum);
    return res;
}

////////////////////////////////////////////////////////////////////////////////

TChecksummableOutput::TChecksummableOutput(TOutputStream* output)
    : Output(output)
    , Checksum(0)
{ }

TChecksum TChecksummableOutput::GetChecksum() const
{
    return Checksum;
}

void TChecksummableOutput::DoWrite(const void* buf, size_t len)
{
    Output->Write(buf, len);
    Checksum = GetChecksumImpl(buf, len, Checksum);
}

void TChecksummableOutput::DoFlush()
{
    Output->Flush();
}

void TChecksummableOutput::DoFinish()
{
    Output->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
