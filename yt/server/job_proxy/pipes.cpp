#include "stdafx.h"
#include "pipes.h"

#include <core/yson/parser.h>
#include <core/yson/consumer.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/sync_reader.h>

#include <core/misc/proc.h>
#include <core/concurrency/scheduler.h>

#include <util/system/file.h>

#include <errno.h>

#if defined(_linux_) || defined(_darwin_)
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/stat.h>
#endif

#if defined(_win_)
    #include <io.h>
#endif

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using NConcurrency::WaitFor;

////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

static const i64 InputBufferSize = (i64) 1 * 1024 * 1024;

static const i64 OutputBufferSize = (i64) 1 * 1024 * 1024;

////////////////////////////////////////////////////////////////////

#if defined(_linux_) || defined(_darwin_)

int SafeDup(int oldFd)
{
    while (true) {
        auto fd = dup(oldFd);

        if (fd == -1) {
            switch (errno) {
            case EINTR:
            case EBUSY:
                break;

            default:
                THROW_ERROR_EXCEPTION("dup failed")
                    << TError::FromSystem();
            }
        } else {
            return fd;
        }
    }
}

void SafeDup2(int oldFd, int newFd)
{
    while (true) {
        auto res = dup2(oldFd, newFd);

        if (res == -1) {
            switch (errno) {
            case EINTR:
            case EBUSY:
                break;

            default:
                THROW_ERROR_EXCEPTION("dup2 failed (OldFd: %d, NewFd: %d)",
                    oldFd,
                    newFd)
                    << TError::FromSystem();
            }
        } else {
            return;
        }
    }
}

int SafePipe(int fd[2])
{
    auto res = pipe(fd);
    if (res == -1) {
        THROW_ERROR_EXCEPTION("pipe failed")
            << TError::FromSystem();
    }
    return res;
}

void SafeMakeNonblocking(int fd)
{
    auto res = fcntl(fd, F_GETFL);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("fcntl failed to get descriptor flags")
            << TError::FromSystem();
    }

    res = fcntl(fd, F_SETFL, res | O_NONBLOCK);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("fcntl failed to set descriptor flags")
            << TError::FromSystem();
    }
}

#else

// Streaming jobs are not supposed to work on windows for now.

int SafeDup(int oldFd)
{
    YUNIMPLEMENTED();
}

void SafeDup2(int oldFd, int newFd)
{
    YUNIMPLEMENTED();
}

int SafePipe(int fd[2])
{
    YUNIMPLEMENTED();
}

void SafeMakeNonblocking(int fd)
{
    YUNIMPLEMENTED();
}

void CheckJobDescriptor(int fd)
{
    YUNIMPLEMENTED();
}

void ChmodJobDescriptor(int fd)
{
    YUNIMPLEMENTED();
}

#endif

////////////////////////////////////////////////////////////////////

void PrepareUserJobPipe(int fd)
{
#ifdef _unix_
    const int permissions = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
    auto procPath = Format("/proc/self/fd/%v", fd);
    auto res = chmod(~procPath, permissions);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("Failed to chmod job descriptor (Fd: %d, Permissions: %d)",
            fd,
            permissions)
            << TError::FromSystem();
    }
#endif
}

////////////////////////////////////////////////////////////////////

TOutputPipe::TOutputPipe(
    int fd[2],
    TOutputStream* output,
    int jobDescriptor)
    : OutputStream(output)
    , JobDescriptor(jobDescriptor)
    , Pipe(fd)
    , IsFinished(false)
    , IsClosed(false)
    , Buffer(OutputBufferSize)
    , Reader(New<NPipes::TAsyncReader>(Pipe.ReadFd))
{
    YCHECK(JobDescriptor);
}

void TOutputPipe::PrepareProxyDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.WriteFd);
    SafeMakeNonblocking(Pipe.ReadFd);
}

TError TOutputPipe::DoAll()
{
    return ReadAll();
}

TError TOutputPipe::ReadAll()
{
    TBlob buffer(InputBufferSize, false);
    while (true)
    {
        auto result = WaitFor(Reader->Read(buffer.Begin(), buffer.Size()));
        RETURN_IF_ERROR(result);

        if (result.Value() == 0) {
            break;
        }

        try {
            OutputStream->Write(buffer.Begin(), result.Value());
        } catch (const std::exception& ex) {
            return TError("Failed to write into output (Fd: %d)",
                JobDescriptor) << TError(ex);
        }
    }
    return TError();
}

TError TOutputPipe::Close()
{
    return Reader->Abort();
}

void TOutputPipe::Finish()
{
    OutputStream->Finish();
}

TJobPipe TOutputPipe::GetJobPipe() const
{
    return TJobPipe{JobDescriptor, Pipe.ReadFd, Pipe.WriteFd};
}

////////////////////////////////////////////////////////////////////

TInputPipe::TInputPipe(
    int fd[2],
    std::unique_ptr<NTableClient::TTableProducer> tableProducer,
    std::unique_ptr<TBlobOutput> buffer,
    std::unique_ptr<NYson::IYsonConsumer> consumer,
    int jobDescriptor)
    : Pipe(fd)
    , JobDescriptor(jobDescriptor)
    , TableProducer(std::move(tableProducer))
    , Buffer(std::move(buffer))
    , Consumer(std::move(consumer))
    , Position(0)
    , HasData(true)
    , IsFinished(false)
    , Writer(New<NPipes::TAsyncWriter>(Pipe.WriteFd))
{
    YCHECK(TableProducer);
    YCHECK(Buffer);
    YCHECK(Consumer);
}

void TInputPipe::PrepareProxyDescriptors()
{
    YASSERT(!IsFinished);

    SafeMakeNonblocking(Pipe.WriteFd);
}

TError TInputPipe::DoAll()
{
    return WriteAll();
}

TError TInputPipe::WriteAll()
{
    while (HasData) {
        HasData = TableProducer->ProduceRow();

        if (HasData && Buffer->Size() < OutputBufferSize) {
            continue;
        }

        if (Buffer->Size() == 0) {
            YCHECK(!HasData);
            continue;
        }

        auto error = WaitFor(Writer->Write(Buffer->Begin(), Buffer->Size()));
        RETURN_IF_ERROR(error);

        Buffer->Clear();
    }

    auto error = WaitFor(Writer->Close());
    return error;
}

TError TInputPipe::Close()
{
    return WaitFor(Writer->Close());
}

void TInputPipe::Finish()
{
    bool dataConsumed = !HasData;
    if (dataConsumed) {
        char buffer;
        // Try to read some data from the pipe.
        ssize_t res = read(Pipe.ReadFd, &buffer, 1);
        dataConsumed = res <= 0;
    }

    SafeClose(Pipe.ReadFd);

    if (!dataConsumed) {
        THROW_ERROR_EXCEPTION("Input stream was not fully consumed by user process (Fd: %d, JobDescriptor: %d)",
            Pipe.WriteFd,
            JobDescriptor);
    }
}

TJobPipe TInputPipe::GetJobPipe() const
{
    return TJobPipe{JobDescriptor, Pipe.ReadFd, Pipe.WriteFd};
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
