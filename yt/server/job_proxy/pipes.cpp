#include "stdafx.h"
#include "pipes.h"

#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/sync_reader.h>

#include <util/system/file.h>

#include <errno.h>

#if defined(_linux_) || defined(_darwin_)
    #include <unistd.h>
    #include <fcntl.h>
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
static const int PipeBufferSize = 1 << 16;

////////////////////////////////////////////////////////////////////

#ifdef _linux_

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
                THROW_ERROR_EXCEPTION("dup2 failed (oldfd: %d, newfd: %d)", oldFd, newFd)
                    << TError::FromSystem();
            }
        } else {
            return;
        }
    }
}

void SafeClose(int fd, bool ignoreInvalidFd)
{
    while (true) {
        auto res = close(fd);
        if (res == -1) { 
            switch (errno) {
            case EINTR:
                break;

            case EBADF:
                if (ignoreInvalidFd) {
                    return;
                } // otherwise fall through and throw exception.

            default:
                THROW_ERROR_EXCEPTION("close failed")
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
        THROW_ERROR_EXCEPTION("Job descriptor is not valid (fd: %d)", fd)
            << TError::FromSystem();
    }

    if (res & FD_CLOEXEC) {
        THROW_ERROR_EXCEPTION("CLOEXEC flag is set for job descriptor (fd: %d)", fd);
    }
}

#elif defined _win_

// Streaming jobs are not supposed to work on windows for now.

int SafeDup(int oldFd)
{
    YUNIMPLEMENTED();
}

void SafeDup2(int oldFd, int newFd)
{
    YUNIMPLEMENTED();
}

void SafeClose(int fd, bool ignoreInvalidFd)
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

#endif

////////////////////////////////////////////////////////////////////

TOutputPipe::TOutputPipe(
    int fd[2],
    TOutputStream* output, 
    int jobDescriptor)
    : OutputStream(output)
    , JobDescriptor(jobDescriptor)
    , IsFinished(false)
    , IsClosed(false)
    , Pipe(fd)
{ }

void TOutputPipe::PrepareJobDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.ReadFd);

    // Always try to close target descriptor before calling dup2.
    SafeClose(JobDescriptor, true);

    SafeDup2(Pipe.WriteFd, JobDescriptor);
    SafeClose(Pipe.WriteFd);

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

    const int bufferSize = 4096;
    char buffer[bufferSize];
    int size;

    for ( ; ; ) {
        size = ::read(Pipe.ReadFd, buffer, bufferSize);

        LOG_TRACE("Read %d bytes from output pipe (JobDescriptor: %d)", size, JobDescriptor);

        if (size > 0) {
            OutputStream->Write(buffer, static_cast<size_t>(size));
            /*if (size == bufferSize) { // it's marginal case
                // try to read again: is more bytes present in pipe?
                // Another way would be to restore this descriptor in epoll
                // and return back to 'read' after epoll's signal
                // (this descriptor in 'event triggered' mode, so restore
                // in epoll indeed required)
                continue;
            }
            return true; */

            continue;
        } else if (size == 0) {
            Close();
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
                    Close();
                    return false;
            }
        }
    }

    return true;
}

void TOutputPipe::Close()
{
    if (IsClosed)
        return;

    SafeClose(Pipe.ReadFd);
    LOG_DEBUG("Output pipe closed (JobDescriptor: %d)", JobDescriptor);
    IsClosed = true;
}

void TOutputPipe::Finish()
{
    if (!IsFinished) {
        Close();

        IsFinished = true;
        OutputStream->Finish();
    }
}

////////////////////////////////////////////////////////////////////

TInputPipe::TInputPipe(
    int fd[2],
    TAutoPtr<NTableClient::TTableProducer> tableProducer,
    TAutoPtr<TBlobOutput> buffer, 
    TAutoPtr<NYTree::IYsonConsumer> consumer,
    int jobDescriptor)
    : TableProducer(tableProducer)
    , Buffer(buffer)
    , Consumer(consumer)
    , JobDescriptor(jobDescriptor)
    , Position(0)
    , IsFinished(false)
    , HasData(true)
    , Pipe(fd)
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
    if (IsFinished)
        return false;

    try {
        while (true) {
            if (Position == Buffer->GetSize()) {
                Position = 0;
                Buffer->Clear();
                while (HasData && Buffer->GetSize() < PipeBufferSize) {
                    HasData = TableProducer->ProduceRow();
                }
            }

            if (Position == Buffer->GetSize()) {
                YCHECK(!HasData);
                SafeClose(Pipe.WriteFd);
                LOG_TRACE("Input pipe finished writing (JobDescriptor: %d)", JobDescriptor);
                return false;
            }

            YASSERT(Position < Buffer->GetSize());

            auto res = ::write(Pipe.WriteFd, Buffer->Begin() + Position, Buffer->GetSize() - Position);
            LOG_TRACE("Written %" PRIPDT " bytes to input pipe (JobDescriptor: %d)", res, JobDescriptor);

            if (res < 0)  {
                if (errno == EAGAIN) {
                    // Pipe blocked, pause writing.
                    return true;
                } else {
                    // Error with pipe.
                    THROW_ERROR_EXCEPTION("Writing to pipe failed (fd: %d, job fd: %d)",
                        Pipe.WriteFd,
                        JobDescriptor)
                        << TError::FromSystem();
                }
            }

            Position += res;
            YASSERT(Position <= Buffer->GetSize());
        }
    } catch (...) {
        ::close(Pipe.WriteFd);
        throw;
    }
}

void TInputPipe::Finish()
{
    // TODO(babenko): eliminate copy-paste
    if (IsFinished)
        return;

    IsFinished = true;
    if (HasData) {
        THROW_ERROR_EXCEPTION("Some data was not consumed by job (fd: %d, job fd: %d)",
            Pipe.WriteFd,
            JobDescriptor);
    }

    // Try to read some data from the pipe.
    char buffer;
    ssize_t res = read(Pipe.ReadFd, &buffer, 1);
    if (res > 0) {
        THROW_ERROR_EXCEPTION("Some data was not consumed by job (fd: %d, job fd: %d)",
            Pipe.WriteFd,
            JobDescriptor);
    }

    SafeClose(Pipe.ReadFd);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
