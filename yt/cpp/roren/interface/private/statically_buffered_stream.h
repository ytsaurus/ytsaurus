#include <util/stream/zerocopy.h>
#include <util/stream/zerocopy_output.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <size_t BufferSize = 128>
class TStaticallyBufferedStream
    : public IZeroCopyOutput
{
public:
    explicit TStaticallyBufferedStream(IOutputStream* underlying)
        : Underlying_(underlying)
    { }

protected:
    size_t DoNext(void** ptr) override
    {
        if (Current() == End()) {
            DoFlush();
        }
        *ptr = Current_;
        size_t result = End() - Current();
        Current_ = End();
        return result;
    }

    void DoUndo(size_t len) override
    {
        Current_ -= len;
        Y_ABORT_UNLESS(Current_ > Buffer_);
    }

    void DoFlush() override
    {
        size_t size = Current() - Begin();
        if (size > 0) {
            Underlying_->Write(Begin(), size);
        }
        Current_ = Begin();
    }

    void DoWrite(const void* data, size_t size) override
    {
        auto toWrite = Min<size_t>(size, End() - Current());
        memcpy(Current_, data, toWrite);
        size -= toWrite;
        Current_ += toWrite;
        data = static_cast<const char*>(data) + toWrite;
        if (size > 0) {
            DoFlush();
            Underlying_->Write(data, size);
        }
    }

private:
    char* Begin()
    {
        return Buffer_;
    }

    char* Current()
    {
        return Current_;
    }

    char* End()
    {
        return Buffer_ + sizeof(Buffer_);
    }

private:
    char Buffer_[BufferSize];
    char* Current_ = Buffer_;

    IOutputStream* Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
