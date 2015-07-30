#include "job_reader.h"
#include <mapreduce/yt/common/log.h>
#include <util/generic/yexception.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TJobReader::TJobReader(int fd)
    : Fd_(fd)
    , PipedInput_(Fd_)
    , BufferedInput_(&PipedInput_, BUFFER_SIZE)
{ }

size_t TJobReader::DoRead(void* buf, size_t len)
{
    return BufferedInput_.Read(buf, len);
}

bool TJobReader::OnStreamError(const yexception& ex)
{
    LOG_ERROR("Read error: %s", ex.what());
    return false;
}

void TJobReader::OnRowFetched()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
