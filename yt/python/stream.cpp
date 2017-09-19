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

TInputStreamWrap::~TInputStreamWrap() throw()
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

TOutputStreamWrap::~TOutputStreamWrap() throw()
{ }

void TOutputStreamWrap::DoWrite(const void* buf, size_t len)
{
    TGilGuard guard;
    WriteFunction_.apply(Py::TupleN(Py::Bytes(
        reinterpret_cast<const char*>(buf),
        len)));
}

////////////////////////////////////////////////////////////////////////////////

struct TInputStreamBlobTag { };

TStreamReader::TStreamReader(TInputStream* stream)
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
    ReadByteCount_ += bytes;
}

bool TStreamReader::IsFinished() const
{
    return Finished_;
}

TSharedRef TStreamReader::ExtractPrefix()
{
    YCHECK(!Blobs_.empty());

    if (!PrefixStart_) {
        PrefixStart_ = Blobs_.front().Begin();
    }

    TSharedMutableRef result;

    if (Blobs_.size() == 1) {
        result = Blobs_[0].Slice(PrefixStart_, CurrentPtr_);
    } else {
        result = TSharedMutableRef::Allocate<TInputStreamBlobTag>(ReadByteCount_, false);

        size_t index = 0;
        auto append = [&] (const char* begin, const char* end) {
            std::copy(begin, end, result.Begin() + index);
            index += end - begin;
        };

        append(PrefixStart_, Blobs_.front().End());
        for (int i = 1; i + 1 < Blobs_.size(); ++i) {
            append(Blobs_[i].Begin(), Blobs_[i].End());
        }
        append(Blobs_.back().Begin(), CurrentPtr_);

        while (Blobs_.size() > 1) {
            Blobs_.pop_front();
        }
    }

    PrefixStart_ = CurrentPtr_;
    ReadByteCount_ = 0;

    return result;
}

void TStreamReader::ReadNextBlob()
{
    NextBlob_ = TSharedMutableRef::Allocate<TInputStreamBlobTag>(BlockSize_, false);
    NextBlobSize_ = Stream_->Load(NextBlob_.Begin(), NextBlob_.Size());
    if (NextBlobSize_ == 0) {
        Finished_ = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
