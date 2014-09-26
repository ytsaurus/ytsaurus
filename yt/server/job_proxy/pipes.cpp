#include "stdafx.h"
#include "pipes.h"

#include <core/yson/parser.h>
#include <core/yson/consumer.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/sync_reader.h>

#include <core/misc/proc.h>

#include <util/system/file.h>

#include <errno.h>

#if defined(_linux_) || defined(_darwin_)
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/stat.h>
#endif

#if defined(_linux_)
    #include <sys/epoll.h>
#endif

#if defined(_win_)
    #include <io.h>
#endif

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////

static auto& Logger = JobProxyLogger;

static const i64 InputBufferSize  = (i64) 1 * 1024 * 1024;
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

void CheckJobDescriptor(int fd)
{
    auto res = fcntl(fd, F_GETFD);
    if (res == -1) {
        THROW_ERROR_EXCEPTION("Job descriptor is not valid (Fd: %d)", fd)
            << TError::FromSystem();
    }

    if (res & FD_CLOEXEC) {
        THROW_ERROR_EXCEPTION("CLOEXEC flag is set for job descriptor (Fd: %d)", fd);
    }
}

void ChmodJobDescriptor(int fd)
{
    const int permissions = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
    auto procPath = Sprintf("/proc/self/fd/%d", fd);
    auto res = chmod(~procPath, permissions);

    if (res == -1) {
        THROW_ERROR_EXCEPTION("Failed to chmod job descriptor (Fd: %d, Permissions: %d)",
            fd,
            permissions)
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
{
    YCHECK(JobDescriptor);
}

void TOutputPipe::PrepareJobDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.ReadFd);

    // Always try to close target descriptor before calling dup2.
    SafeClose(JobDescriptor, true);

    SafeDup2(Pipe.WriteFd, JobDescriptor);
    SafeClose(Pipe.WriteFd);

    ChmodJobDescriptor(JobDescriptor);

    CheckJobDescriptor(JobDescriptor);
}

void TOutputPipe::PrepareProxyDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.WriteFd);
    SafeMakeNonblocking(Pipe.ReadFd);
}

int TOutputPipe::GetEpollDescriptor() const
{
    YASSERT(!IsFinished);

    return Pipe.ReadFd;
}

int TOutputPipe::GetEpollFlags() const
{
    YASSERT(!IsFinished);

#ifdef _linux_
    return EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLET;
#else
    YUNIMPLEMENTED();
#endif
}

bool TOutputPipe::ProcessData(ui32 epollEvent)
{
    YASSERT(!IsFinished);

    while (true) {
        int bytesRead = ::read(Pipe.ReadFd, Buffer.Begin(), Buffer.Size());

        LOG_TRACE("Read %d bytes from output pipe (JobDescriptor: %d)",
            bytesRead,
            JobDescriptor);

        if (bytesRead > 0) {
            try {
                OutputStream->Write(Buffer.Begin(), static_cast<size_t>(bytesRead));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to write into output (Fd: %d)",
                    JobDescriptor) << ex;
            }
        } else if (bytesRead == 0) {
            return false;
        } else { // size < 0
            switch (errno) {
                case EAGAIN:
                    errno = 0; // this is NONBLOCK socket, nothing read; return
                    return true;
                case EINTR:
                    // retry
                    break;
                default:
                    return false;
            }
        }
    }

    return true;
}

void TOutputPipe::CloseHandles()
{
    SafeClose(Pipe.ReadFd);
    LOG_DEBUG("Output pipe closed (JobDescriptor: %d)",
        JobDescriptor);
}

void TOutputPipe::Finish()
{
    OutputStream->Finish();
}

////////////////////////////////////////////////////////////////////

TInputPipe::TInputPipe(
    int fd[2],
    std::unique_ptr<NTableClient::TTableProducer> tableProducer,
    std::unique_ptr<TBlobOutput> buffer,
    std::unique_ptr<NYson::IYsonConsumer> consumer,
    int jobDescriptor,
    bool checkDataFullyConsumed)
    : Pipe(fd)
    , JobDescriptor(jobDescriptor)
    , TableProducer(std::move(tableProducer))
    , Buffer(std::move(buffer))
    , Consumer(std::move(consumer))
    , CheckDataFullyConsumed(checkDataFullyConsumed)
    , Position(0)
    , HasData(true)
    , IsFinished(false)
{
    YCHECK(~TableProducer);
    YCHECK(~Buffer);
    YCHECK(~Consumer);
}

void TInputPipe::PrepareJobDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.WriteFd);

    // Always try to close target descriptor before calling dup2.
    SafeClose(JobDescriptor, true);

    SafeDup2(Pipe.ReadFd, JobDescriptor);
    SafeClose(Pipe.ReadFd);

    ChmodJobDescriptor(JobDescriptor);

    CheckJobDescriptor(JobDescriptor);
}

void TInputPipe::PrepareProxyDescriptors()
{
    YASSERT(!IsFinished);

    SafeMakeNonblocking(Pipe.WriteFd);
}

int TInputPipe::GetEpollDescriptor() const
{
    YASSERT(!IsFinished);

    return Pipe.WriteFd;
}

int TInputPipe::GetEpollFlags() const
{
    YASSERT(!IsFinished);

#ifdef _linux_
    return EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLET;
#else
    YUNIMPLEMENTED();
#endif
}

bool TInputPipe::ProcessData(ui32 epollEvents)
{
    if (IsFinished) {
        return false;
    }

    while (true) {
        if (Position == Buffer->Size()) {
            Position = 0;
            swap(PreviousBuffer, *Buffer);
            Buffer->Clear();
            while (HasData && Buffer->Size() < InputBufferSize) {
                HasData = TableProducer->ProduceRow();
            }
        }

        if (Position == Buffer->Size()) {
            YCHECK(!HasData);
            LOG_TRACE("Input pipe finished writing (JobDescriptor: %d)",
                JobDescriptor);
            return false;
        }

        YASSERT(Position < Buffer->Size());

        auto res = ::write(Pipe.WriteFd, Buffer->Begin() + Position, Buffer->Size() - Position);

        if (res < 0)  {
            if (errno == EAGAIN) {
                // Pipe blocked, pause writing.
                return true;
            } else {
                // Error with pipe.
                THROW_ERROR_EXCEPTION("Writing to pipe failed (Fd: %d, JobDescriptor: %d)",
                    Pipe.WriteFd,
                    JobDescriptor)
                    << TError::FromSystem();
            }
        }
        LOG_TRACE("Written %" PRISZT " bytes to input pipe (JobDescriptor: %d)",
            res,
            JobDescriptor);

        Position += res;
        YASSERT(Position <= Buffer->Size());
    }

}

void TInputPipe::CloseHandles()
{
    LOG_DEBUG("Input pipe closed (JobDescriptor: %d)",
        JobDescriptor);
    SafeClose(Pipe.WriteFd);
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

    if (!dataConsumed && CheckDataFullyConsumed) {
        THROW_ERROR_EXCEPTION("Input stream was not fully consumed by user process (Fd: %d, JobDescriptor: %d)",
            Pipe.WriteFd,
            JobDescriptor);
    }
}

TBlob TInputPipe::GetFailContext() const
{
    TBlob result;
    result.Append(TRef::FromBlob(PreviousBuffer.Blob()));
    result.Append(TRef::FromBlob(Buffer->Blob()));
    return result;
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
