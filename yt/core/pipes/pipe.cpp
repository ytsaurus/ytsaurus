#include "pipe.h"
#include "private.h"
#include "io_dispatcher.h"

#include <yt/core/net/connection.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/fs.h>

#include <sys/types.h>
#include <sys/stat.h>

namespace NYT::NPipes {

using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = PipesLogger;

////////////////////////////////////////////////////////////////////////////////

TNamedPipe::TNamedPipe(const TString& path)
    : Path_(path)
{ }

TNamedPipe::~TNamedPipe()
{
    if (unlink(Path_.c_str()) == -1) {
        YT_LOG_INFO(TError::FromSystem(), "Failed to unlink pipe %v", Path_);
    }
}

TNamedPipePtr TNamedPipe::Create(const TString& path)
{
    auto pipe = New<TNamedPipe>(path);
    pipe->Open();
    YT_LOG_DEBUG("Named pipe created (Path: %v)", path);
    return pipe;
}

void TNamedPipe::Open()
{
    if (mkfifo(Path_.c_str(), 0660) == -1) {
        THROW_ERROR_EXCEPTION("Failed to create named pipe %v", Path_)
            << TError::FromSystem();
    }
}

IConnectionReaderPtr TNamedPipe::CreateAsyncReader()
{
    YCHECK(!Path_.empty());
    return CreateInputConnectionFromPath(Path_, TIODispatcher::Get()->GetPoller(), MakeStrong(this));
}

IConnectionWriterPtr TNamedPipe::CreateAsyncWriter()
{
    YCHECK(!Path_.empty());
    return CreateOutputConnectionFromPath(Path_, TIODispatcher::Get()->GetPoller(), MakeStrong(this));
}

TString TNamedPipe::GetPath() const
{
    return Path_;
}

////////////////////////////////////////////////////////////////////////////////

TPipe::TPipe()
{ }

TPipe::TPipe(TPipe&& pipe)
{
    Init(std::move(pipe));
}

TPipe::TPipe(int fd[2])
    : ReadFD_(fd[0])
    , WriteFD_(fd[1])
{ }

void TPipe::Init(TPipe&& other)
{
    ReadFD_ = other.ReadFD_;
    WriteFD_ = other.WriteFD_;
    other.ReadFD_ = InvalidFD;
    other.WriteFD_ = InvalidFD;
}

TPipe::~TPipe()
{
    if (ReadFD_ != InvalidFD) {
        YCHECK(TryClose(ReadFD_, false));
    }

    if (WriteFD_ != InvalidFD) {
        YCHECK(TryClose(WriteFD_, false));
    }
}

void TPipe::operator=(TPipe&& other)
{
    if (this == &other) {
        return;
    }

    Init(std::move(other));
}

IConnectionWriterPtr TPipe::CreateAsyncWriter()
{
    YCHECK(WriteFD_ != InvalidFD);
    SafeMakeNonblocking(WriteFD_);
    return CreateConnectionFromFD(ReleaseWriteFD(), {}, {}, TIODispatcher::Get()->GetPoller());
}

IConnectionReaderPtr TPipe::CreateAsyncReader()
{
    YCHECK(ReadFD_ != InvalidFD);
    SafeMakeNonblocking(ReadFD_);
    return CreateConnectionFromFD(ReleaseReadFD(), {}, {}, TIODispatcher::Get()->GetPoller());
}

int TPipe::ReleaseReadFD()
{
    YCHECK(ReadFD_ != InvalidFD);
    auto fd = ReadFD_;
    ReadFD_ = InvalidFD;
    return fd;
}

int TPipe::ReleaseWriteFD()
{
    YCHECK(WriteFD_ != InvalidFD);
    auto fd = WriteFD_;
    WriteFD_ = InvalidFD;
    return fd;
}

int TPipe::GetReadFD() const
{
    YCHECK(ReadFD_ != InvalidFD);
    return ReadFD_;
}

int TPipe::GetWriteFD() const
{
    YCHECK(WriteFD_ != InvalidFD);
    return WriteFD_;
}

void TPipe::CloseReadFD()
{
    if (ReadFD_ == InvalidFD) {
        return;
    }
    auto fd = ReadFD_;
    ReadFD_ = InvalidFD;
    SafeClose(fd, false);
}

void TPipe::CloseWriteFD()
{
    if (WriteFD_ == InvalidFD) {
        return;
    }
    auto fd = WriteFD_;
    WriteFD_ = InvalidFD;
    SafeClose(fd, false);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TPipe& pipe)
{
    return Format("{ReadFD: %v, WriteFD: %v}",
        pipe.GetReadFD(),
        pipe.GetWriteFD());
}

////////////////////////////////////////////////////////////////////////////////

TPipeFactory::TPipeFactory(int minFD)
    : MinFD_(minFD)
{ }

TPipeFactory::~TPipeFactory()
{
    for (int fd : ReservedFDs_) {
        YCHECK(TryClose(fd, false));
    }
}

TPipe TPipeFactory::Create()
{
    while (true) {
        int fd[2];
        SafePipe(fd);
        if (fd[0] >= MinFD_ && fd[1] >= MinFD_) {
            TPipe pipe(fd);
            return pipe;
        } else {
            ReservedFDs_.push_back(fd[0]);
            ReservedFDs_.push_back(fd[1]);
        }
    }
}

void TPipeFactory::Clear()
{
    for (int& fd : ReservedFDs_) {
        YCHECK(TryClose(fd, false));
        fd = TPipe::InvalidFD;
    }
    ReservedFDs_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
