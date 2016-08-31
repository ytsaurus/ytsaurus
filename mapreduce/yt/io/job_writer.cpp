#include "job_writer.h"

#include <mapreduce/yt/interface/io.h>

#include <util/stream/pipe.h>
#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobWriter::TStream::TStream(int fd)
    : PipedOutput(new TPipedOutput(fd))
    , BufferedOutput(new TBufferedOutput(PipedOutput.Get(), BUFFER_SIZE))
{ }

TJobWriter::TJobWriter(size_t outputTableCount)
{
    for (size_t i = 0; i < outputTableCount; ++i) {
        Streams_.emplace_back(i * 3 + 1);
    }
}

size_t TJobWriter::GetStreamCount() const
{
    return Streams_.size();
}

TOutputStream* TJobWriter::GetStream(size_t tableIndex) const
{
    if (tableIndex >= Streams_.size()) {
        ythrow TIOException() <<
            "Table index " << tableIndex <<
            " is out of range [0, " << Streams_.size() << ")";
    }
    return Streams_[tableIndex].BufferedOutput.Get();
}

void TJobWriter::OnRowFinished(size_t)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
