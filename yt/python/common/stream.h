#pragma once

#include <yt/core/misc/ref.h>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

#include <pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamWrap
    : public IInputStream
{
public:
    explicit TInputStreamWrap(const Py::Object& inputStream);
    virtual ~TInputStreamWrap() noexcept;

    virtual size_t DoRead(void* buf, size_t len);

private:
    Py::Object InputStream_;
    Py::Callable ReadFunction_;
};

class TOutputStreamWrap: public IOutputStream {
public:
    explicit TOutputStreamWrap(const Py::Object& outputStream);
    virtual ~TOutputStreamWrap() noexcept;

    virtual void DoWrite(const void* buf, size_t len);

private:
    Py::Object OutputStream_;
    Py::Callable WriteFunction_;
};

class TOwningStringInput
    : public IInputStream
{
public:
    explicit TOwningStringInput(const TString& string)
        : String_(string)
        , Stream_(String_)
    { }

private:
    virtual size_t DoRead(void* buf, size_t len) override
    {
        return Stream_.Read(buf, len);
    }

    TString String_;
    TStringInput Stream_;
};

////////////////////////////////////////////////////////////////////////////////

class TStreamReader
{
public:
    TStreamReader()
    { }

    explicit TStreamReader(IInputStream* stream);

    const char* Begin() const;
    const char* Current() const;
    const char* End() const;

    void RefreshBlock();
    void Advance(size_t bytes);

    bool IsFinished() const;
    TSharedRef ExtractPrefix(const char* endPtr);

private:
    IInputStream* Stream_;

    std::deque<TSharedRef> Blobs_;

    TSharedRef NextBlob_;
    i64 NextBlobSize_ = 0;

    const char* BeginPtr_ = nullptr;
    const char* CurrentPtr_ = nullptr;
    const char* EndPtr_ = nullptr;

    const char* PrefixStart_ = nullptr;

    bool Finished_ = false;
    static const size_t BlockSize_ = 1024 * 1024;

    void ReadNextBlob();
};
////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

