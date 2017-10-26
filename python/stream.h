#pragma once

#include <yt/core/misc/ref.h>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

#include <contrib/libs/pycxx/Objects.hxx>

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
    explicit TStreamReader(IInputStream* stream);

    const char* Begin() const;
    const char* Current() const;
    const char* End() const;

    void RefreshBlock();
    void Advance(size_t bytes);

    bool IsFinished() const;
    TSharedRef ExtractPrefix();

private:
    IInputStream* Stream_;

    std::deque<TSharedMutableRef> Blobs_;

    TSharedMutableRef NextBlob_;
    i64 NextBlobSize_ = 0;

    char* BeginPtr_ = nullptr;
    char* CurrentPtr_ = nullptr;
    char* EndPtr_ = nullptr;

    char* PrefixStart_ = nullptr;
    i64 ReadByteCount_ = 0;

    bool Finished_ = false;
    static const size_t BlockSize_ = 1024 * 1024;

    void ReadNextBlob();
};
////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

