#include "stream_output.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void TFixedBufferFileOutput::DoWrite(const void* buf, size_t len)
{
    Underlying_.Write(buf, len);
}

void TFixedBufferFileOutput::DoFlush()
{
    Underlying_.Flush();
}

void TFixedBufferFileOutput::DoFinish()
{
    Underlying_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
