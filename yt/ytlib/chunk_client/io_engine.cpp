#include "io_engine.h"

#include <util/system/platform.h>

#ifdef _linux_
#include <linux/aio_abi.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <yt/core/misc/align.h>
#include <yt/core/misc/error.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TAioEngineDataBufferTag {};
struct TDefaultEngineDataBufferTag {};

#ifdef _linux_

inline int io_setup(unsigned nr, aio_context_t* ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

inline int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

inline int io_submit(aio_context_t ctx, long nr,  struct iocb** iocbpp) {
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

inline int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
                        struct io_event* events, struct timespec* timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

#endif

class TDefaultIOEngine
    : public IIOEngine
{
public:
    TDefaultIOEngine() = default;

    virtual size_t Pread(const TFile& handle, TSharedMutableRef & result, size_t len, i64 offset) override;
    virtual void Pwrite(const TFile& handle, const void* buf, size_t len, i64 offset) override;
    virtual std::unique_ptr<TFile> Open(const TString& fName, EOpenMode oMode) override;
};

size_t TDefaultIOEngine::Pread(const TFile& handle, TSharedMutableRef& result, size_t len, i64 offset)
{
    result = TSharedMutableRef::Allocate<TDefaultEngineDataBufferTag>(len, false);
    auto size = handle.Pread(result.Begin(), len, offset);
    result = result.Slice(0, size);
    return size;
}

void TDefaultIOEngine::Pwrite(const TFile& handle, const void* buf, size_t len, i64 offset)
{
    handle.Pwrite(buf, len, offset);
}

std::unique_ptr<TFile> TDefaultIOEngine::Open(const TString& fName, EOpenMode oMode)
{
    return std::make_unique<TFile>(fName, oMode);
}

#ifdef _linux_

class TAioEngine
    : public IIOEngine
{
public:
    TAioEngine();
    ~TAioEngine();

    virtual size_t Pread(const TFile& handle, TSharedMutableRef& result, size_t len, i64 offset) override;
    virtual void Pwrite(const TFile& handle, const void* buf, size_t len, i64 offset) override;
    virtual std::unique_ptr<TFile> Open(const TString& fName, EOpenMode oMode) override;

private:
    void SubmitRead(FHANDLE fd, void* p, size_t len, i64 offset);
    void MaybeWait();
    void Wait();

    aio_context_t Ctx_ = 0;
    bool Initialized_ = false;
    const int MaxQueueSize_ = 128;
    int Submitted_ = 0;
};

TAioEngine::TAioEngine()
{
    int ret = io_setup(MaxQueueSize_, &Ctx_);
    if (ret < 0) {
        THROW_ERROR_EXCEPTION("Cannot initialize aio") << TError::FromSystem();
    }
}

TAioEngine::~TAioEngine()
{
    io_destroy(Ctx_);
}

void TAioEngine::SubmitRead(FHANDLE fd, void* p, size_t len, i64 offset)
{
    struct iocb cb;
    struct iocb* cbs[1];
    memset(&cb, 0, sizeof(cb));

    cb.aio_fildes = fd;
    cb.aio_lio_opcode = IOCB_CMD_PREAD;

    cb.aio_buf = reinterpret_cast<ui64>(p);
    cb.aio_offset = offset;
    cb.aio_nbytes = len;

    cbs[0] = &cb;

    int ret = io_submit(Ctx_, 1, cbs);
    if (ret != 1) {
        THROW_ERROR_EXCEPTION("Cannot read %Qv bytes from file", len) << TError::FromSystem();
    }

    ++Submitted_;
}

void TAioEngine::MaybeWait() {
    if (Submitted_ > MaxQueueSize_ - 1) {
        Wait();
    }
}

void TAioEngine::Wait()
{
    if (!Submitted_) {
        return;
    }

    struct io_event events[Submitted_];
    int ret = io_getevents(Ctx_, Submitted_, Submitted_, events, NULL);
    if (ret < 0) {
        THROW_ERROR_EXCEPTION("Cannot handle IO requests") << TError::FromSystem();
    }
    Submitted_ = 0;
}

size_t TAioEngine::Pread(const TFile& handle, TSharedMutableRef& result, size_t len, i64 offset)
{
    const int alignment = 512; // TODO:
    const int readBlock = 4096;

    auto tmp = TSharedMutableRef::Allocate<TAioEngineDataBufferTag>(len + 3*alignment, false);
    tmp = tmp.Slice(AlignUp(tmp.Begin(), alignment), tmp.End());

    auto f = handle.GetHandle();

    i64 from = offset;
    from -= offset % alignment;
    i64 to = offset + len;

    char * p = tmp.Begin();

    while (from < to) {
        size_t bytesToRead = from + readBlock <= to ? readBlock : alignment;
        SubmitRead(f, p, bytesToRead, from);

        p += bytesToRead;
        from += bytesToRead;

        MaybeWait();
    }

    Wait();

    result = tmp.Slice(offset % alignment, offset % alignment + len);
    return len;
}

void TAioEngine::Pwrite(const TFile& handle, const void* buf, size_t len, i64 offset)
{
    // TODO:
    handle.Pwrite(buf, len, offset);
}

std::unique_ptr<TFile> TAioEngine::Open(const TString& fName, EOpenMode oMode)
{
    return std::make_unique<TFile>(fName, oMode | DirectAligned);
}

#endif

IIOEnginePtr CreateDefaultIOEngine()
{
    return New<TDefaultIOEngine>();
}

IIOEnginePtr CreateIOEngine(const TString & name)
{
    if (name == "default") {
        return New<TDefaultIOEngine>();
#ifdef _linux_
    } else if (name == "aio") {
        return New<TAioEngine>();
#endif
    } else {
        THROW_ERROR_EXCEPTION("unknown io engine %Qv", name);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
