#include "stream.h"
#include "helpers.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <contrib/libs/pycxx/Objects.hxx>

#include <iostream>
#include <string>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

TInputStreamWrap::TInputStreamWrap(const Py::Object& inputStream)
    : InputStream_(inputStream)
    , ReadFunction_(InputStream_.getAttr("read"))
{ }

TInputStreamWrap::~TInputStreamWrap() noexcept
{ }

size_t TInputStreamWrap::DoRead(void* buf, size_t len)
{
    TGilGuard guard;

    auto args = Py::TupleN(Py::Long(static_cast<long>(len)));
    Py::Object result = ReadFunction_.apply(args);

    PyObject* exception = PyErr_Occurred();
    if (exception) {
        throw Py::Exception();
    }

#if PY_MAJOR_VERSION < 3
    // COMPAT: Due to implicit promotion to unicode it is sane to work with
    // unicode objects too.
    if (!PyBytes_Check(result.ptr()) && !PyUnicode_Check(result.ptr())) {
        throw Py::TypeError("Read returns non-string object");
    }
#else
    if (!PyBytes_Check(result.ptr())) {
        throw Py::TypeError("Input stream should be binary");
    }
#endif
    auto data = PyBytes_AsString(*result);
    auto length = PyBytes_Size(*result);
    std::copy(data, data + length, (char*)buf);
    return length;
}

TOutputStreamWrap::TOutputStreamWrap(const Py::Object& outputStream)
    : OutputStream_(outputStream)
    , WriteFunction_(OutputStream_.getAttr("write"))
{ }

TOutputStreamWrap::~TOutputStreamWrap() noexcept
{ }

void TOutputStreamWrap::DoWrite(const void* buf, size_t len)
{
    TGilGuard guard;

    size_t index = 0;
    while (len > 0) {
        size_t toWrite = std::min(len, static_cast<size_t>(1ll << 30));
        WriteFunction_.apply(Py::TupleN(Py::Bytes(
            reinterpret_cast<const char*>(buf) + index,
            toWrite)));
        len -= toWrite;
        index += toWrite;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TInputStreamBlobTag { };

TStreamReader::TStreamReader(IInputStream* stream)
    : Stream_(stream)
{
    ReadNextBlob();
    if (!Finished_) {
        RefreshBlock();
    }
}

const char* TStreamReader::Begin() const
{
    return BeginPtr_;
}

const char* TStreamReader::Current() const
{
    return CurrentPtr_;
}

const char* TStreamReader::End() const
{
    return EndPtr_;
}

void TStreamReader::RefreshBlock()
{
    YCHECK(CurrentPtr_ == EndPtr_);
    YCHECK(!Finished_);

    Blobs_.push_back(NextBlob_);
    BeginPtr_ = NextBlob_.Begin();
    CurrentPtr_ = BeginPtr_;
    EndPtr_ = NextBlob_.Begin() + NextBlobSize_;

    if (NextBlobSize_ < BlockSize_) {
        Finished_ = true;
    } else {
        ReadNextBlob();
    }
}

void TStreamReader::Advance(size_t bytes)
{
    CurrentPtr_ += bytes;
}

bool TStreamReader::IsFinished() const
{
    return Finished_;
}

TSharedRef TStreamReader::ExtractPrefix(const char* endPtr)
{
    if (Blobs_.empty()) {
        return TSharedRef();
    }

    YCHECK(endPtr <= Blobs_.back().End());
    YCHECK(endPtr >= Blobs_.back().Begin());

    if (!PrefixStart_) {
        PrefixStart_ = Blobs_.front().Begin();
    }

    TSharedRef result;

    if (Blobs_.size() == 1) {
        result = Blobs_[0].Slice(PrefixStart_, endPtr);
    } else {
        auto firstBlockSuffixLength = Blobs_.front().End() - PrefixStart_;
        auto lastBlockPrefixLength = endPtr - Blobs_.back().Begin();

        auto prefixLength = firstBlockSuffixLength + (Blobs_.size() - 2) * BlockSize_ + lastBlockPrefixLength;

        auto prefix = TSharedMutableRef::Allocate<TInputStreamBlobTag>(prefixLength, false);

        size_t index = 0;
        auto append = [&] (const char* begin, const char* end) {
            std::copy(begin, end, prefix.Begin() + index);
            index += end - begin;
        };

        append(PrefixStart_, Blobs_.front().End());
        for (int i = 1; i + 1 < Blobs_.size(); ++i) {
            append(Blobs_[i].Begin(), Blobs_[i].End());
        }
        append(Blobs_.back().Begin(), endPtr);
        while (Blobs_.size() > 1) {
            Blobs_.pop_front();
        }
        result = prefix;
    }

    PrefixStart_ = endPtr;
    return result;
}

TSharedRef TStreamReader::ExtractPrefix()
{
    return ExtractPrefix(CurrentPtr_);
}

void TStreamReader::ReadNextBlob()
{
    auto blob = TSharedMutableRef::Allocate<TInputStreamBlobTag>(BlockSize_, false);
    NextBlobSize_ = Stream_->Load(blob.Begin(), blob.Size());
    NextBlob_ = blob;
    if (NextBlobSize_ == 0) {
        Finished_ = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
