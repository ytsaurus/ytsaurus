#include "stdafx.h"
#include "pipes.h"

#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/sync_reader.h>

#include <util/system/file.h>

#include <errno.h>

#ifdef _linux_
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/epoll.h>
#else
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
    auto fd = dup(oldFd);

    // ToDo: provide proper message.
    if (fd == -1) {
        ythrow yexception() << "dup failed with errno: " << errno;
    }
    return fd;
}

void SafeDup2(int oldFd, int newFd)
{
    auto res = dup2(oldFd, newFd);

    // ToDo: provide proper message.
    if (res == -1) {
        ythrow yexception() << "dup2 failed with errno: " << errno;
    }
}

void SafeClose(int fd)
{
    auto res = close(fd);

    // ToDo: provide proper message.
    if (res == -1) {
        ythrow yexception() <<
            Sprintf("close failed with errno: %d, fd: %d", errno, fd);
    }
}

int SafePipe(int fd[2])
{
    auto res = pipe(fd);

    // ToDo: provide proper message.
    if (res == -1) {
        ythrow yexception() << "pipe failed with errno: " << errno;
    }

    return res;
}

void SafeMakeNonblocking(int fd)
{
    auto res = fcntl(fd, F_GETFL);

    if (res == -1)
        ythrow yexception() << Sprintf(
            "fcntl failed to get descriptor flags (fd: %d, errno: %d)",
            fd, 
            errno);

    res = fcntl(fd, F_SETFL, res | O_NONBLOCK);

    if (res == -1)
        ythrow yexception() << Sprintf(
            "fcntl failed to set descriptor to nonblocking mode (fd: %d, errno %d)",
            fd,
            errno);
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

void SafeClose(int fd)
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

#endif

////////////////////////////////////////////////////////////////////

TOutputPipe::TOutputPipe(
    TOutputStream* output, 
    int jobDescriptor)
    : OutputStream(output)
    , JobDescriptor(jobDescriptor)
    , IsFinished(false)
{
    int fd[2];
    SafePipe(fd);
    Pipe = TPipe(fd);
}

void TOutputPipe::PrepareJobDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.ReadFd);
    SafeDup2(Pipe.WriteFd, JobDescriptor);
    SafeClose(Pipe.WriteFd);
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
            errno = 0;
            SafeClose(Pipe.ReadFd);

            LOG_TRACE("Output pipe closed (JobDescriptor: %d)", JobDescriptor);

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
                    SafeClose(Pipe.ReadFd);

                    LOG_TRACE("Output pipe closed (JobDescriptor: %d)", JobDescriptor);

                    return false;
            }
        }
    }

    return true;
}

void TOutputPipe::Finish()
{
    if (!IsFinished) {
        IsFinished = true;
        OutputStream->Finish();
    }
}

////////////////////////////////////////////////////////////////////

TInputPipe::TInputPipe(
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
{
    int fd[2];
    SafePipe(fd);
    Pipe = TPipe(fd);
}

void TInputPipe::PrepareJobDescriptors()
{
    YASSERT(!IsFinished);

    SafeClose(Pipe.WriteFd);
    SafeDup2(Pipe.ReadFd, JobDescriptor);
    SafeClose(Pipe.ReadFd);
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
    YASSERT(!IsFinished);

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
            return true;
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
                ythrow yexception() << 
                    Sprintf("Writing to pipe failed (fd: %d, job fd: %d, errno: %d).",
                    Pipe.WriteFd,
                    JobDescriptor,
                    errno);
            }
        }

        Position += res;
        YASSERT(Position <= Buffer->GetSize());
    }
}

void TInputPipe::Finish()
{
    // TODO(babenko): eliminate copy-paste

    if (HasData) {
        ythrow yexception() << Sprintf("Not all data was consumed by job (fd: %d, job fd: %d)",
            Pipe.WriteFd,
            JobDescriptor);
    }

    // Try to read some data from the pipe.
    char buffer;
    ssize_t res = read(Pipe.ReadFd, &buffer, 1);
    if (res > 0) {
        ythrow yexception() << Sprintf("Not all data was consumed by job (fd: %d, job fd: %d)",
            Pipe.WriteFd,
            JobDescriptor);
    }

    SafeClose(Pipe.ReadFd);
    IsFinished = true;
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
