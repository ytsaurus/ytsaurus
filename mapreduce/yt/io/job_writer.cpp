#include "job_writer.h"

#include <util/stream/pipe.h>
#include <util/stream/buffered.h>

#include <mapreduce/yt/common/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobWriter::TStream::TStream(int fd)
    : PipedOutput(new TPipedOutput(fd))
    , BufferedOutput(new TBufferedOutput(PipedOutput.Get(), BUFFER_SIZE))
{ }

TJobWriter::TJobWriter(size_t outputTableCount)
{
    for (size_t i = 0; i < outputTableCount; ++i) {
        Streams_.push_back(MoveArg(TStream(i * 3 + 1)));
    }
}

size_t TJobWriter::GetStreamCount() const
{
    return Streams_.size();
}

TOutputStream* TJobWriter::GetStream(size_t tableIndex)
{
    if (tableIndex >= Streams_.size()) {
        FAIL("Table index %" PRISZT " is out of range", tableIndex);
    }
    return Streams_[tableIndex].BufferedOutput.Get();
}

void TJobWriter::OnRowFinished(size_t)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
