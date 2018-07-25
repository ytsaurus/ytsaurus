#include "stream.h"
#include "helpers.h"

#include <yt/core/misc/blob_output.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>

#include <Objects.hxx> // pycxx

#include <memory>
#include <string>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TInputStreamForwarder
    : public IInputStream
{
public:
    explicit TInputStreamForwarder(const Py::Object& inputStream)
        : InputStream_(inputStream)
        , ReadFunction_(InputStream_.getAttr("read"))
    { }

    virtual ~TInputStreamForwarder() noexcept = default;

    size_t DoRead(void* buf, size_t len)
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

private:
    Py::Object InputStream_;
    Py::Callable ReadFunction_;
};

std::unique_ptr<IInputStream> CreateInputStreamWrapper(const Py::Object& pythonInputStream)
{
#if PY_MAJOR_VERSION < 3
    if (PyFile_Check(pythonInputStream.ptr())) {
        FILE* file = PyFile_AsFile(pythonInputStream.ptr());
        return std::make_unique<TFileInput>(Duplicate(file));
    } else {
        return std::make_unique<TInputStreamForwarder>(pythonInputStream);
    }
#else
    return std::make_unique<TInputStreamForwarder>(pythonInputStream);
#endif
}

////////////////////////////////////////////////////////////////////////////////

class TOutputStreamForwarder
    : public IOutputStream
{
public:
    explicit TOutputStreamForwarder(const Py::Object& outputStream)
        : OutputStream_(outputStream)
        , WriteFunction_(OutputStream_.getAttr("write"))
    { }

    virtual ~TOutputStreamForwarder() noexcept = default;

    void DoWrite(const void* buf, size_t len)
    {
        TGilGuard guard;

        size_t index = 0;
        while (len > 0) {
            // NB: python string interface uses i32 for length.
            size_t toWrite = std::min(len, static_cast<size_t>(1_GB));
            WriteFunction_.apply(Py::TupleN(Py::Bytes(
                reinterpret_cast<const char*>(buf) + index,
                toWrite)));
            len -= toWrite;
            index += toWrite;
        }
    }

private:
    Py::Object OutputStream_;
    Py::Callable WriteFunction_;
};

class TBufferedStreamWrapper
    : public IOutputStream
{
public:
    explicit TBufferedStreamWrapper(std::unique_ptr<IOutputStream> outputStreamHolder)
        : UnderlyingOutputStream_(std::move(outputStreamHolder))
        , BufferedOutputStream_(UnderlyingOutputStream_.get(), 1_MB)
    { }

    void DoWrite(const void* buf, size_t len)
    {
        BufferedOutputStream_.Write(buf, len);
    }

private:
    std::unique_ptr<IOutputStream> UnderlyingOutputStream_;
    TBufferedOutput BufferedOutputStream_;
};

std::unique_ptr<IOutputStream> CreateOutputStreamWrapper(const Py::Object& pythonOutputStream, bool addBuffering)
{
#if PY_MAJOR_VERSION < 3
    std::unique_ptr<IOutputStream> outputStreamHolder;
    if (PyFile_Check(pythonOutputStream.ptr())) {
        FILE* file = PyFile_AsFile(pythonOutputStream.ptr());
        outputStreamHolder = std::make_unique<TFileOutput>(Duplicate(file));
    } else {
        outputStreamHolder = std::make_unique<TOutputStreamForwarder>(pythonOutputStream);
    }
    if (addBuffering) {
        return std::make_unique<TBufferedStreamWrapper>(std::move(outputStreamHolder));
    } else {
        return outputStreamHolder;
    }
#else
    // Python 3 has "io" module with fine-grained buffering control, no need in
    // additional buferring here.
    return std::unique_ptr<TOutputStreamForwarder>(new TOutputStreamForwarder(pythonOutputStream));
#endif
}

////////////////////////////////////////////////////////////////////////////////

class TOwningStringInput
    : public IInputStream
{
public:
    explicit TOwningStringInput(TString string)
        : String_(std::move(string))
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

std::unique_ptr<IInputStream> CreateOwningStringInput(TString string)
{
    return std::unique_ptr<IInputStream>(new TOwningStringInput(string));
}

////////////////////////////////////////////////////////////////////////////////

struct TInputStreamBlobTag { };

TStreamReader::TStreamReader(IInputStream* stream)
    : Stream_(stream)
{
    ReadNextBlock();
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

    Blocks_.push_back(NextBlock_);
    if (Blocks_.size() == 1) {
        PrefixStart_ = Blocks_[0].begin();
    }

    BeginPtr_ = NextBlock_.Begin();
    CurrentPtr_ = BeginPtr_;
    EndPtr_ = NextBlock_.Begin() + NextBlockSize_;

    if (NextBlockSize_ < BlockSize_) {
        Finished_ = true;
    } else {
        ReadNextBlock();
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

TSharedRef TStreamReader::ExtractPrefix(int endBlockIndex, const char* endPtr)
{
    TSharedRef result;

    if (endBlockIndex == 0) {
        result = Blocks_[0].Slice(PrefixStart_, endPtr);
    } else {
        auto firstBlockSuffixLength = Blocks_[0].End() - PrefixStart_;
        auto lastBlockPrefixLength = endPtr - Blocks_[endBlockIndex].Begin();
        auto prefixLength = firstBlockSuffixLength + (endBlockIndex - 1) * BlockSize_ + lastBlockPrefixLength;
        TBlobOutput prefixOutput(prefixLength);

        prefixOutput.Write(PrefixStart_, Blocks_[0].End() - PrefixStart_);
        for (int i = 1; i + 1 < endBlockIndex; ++i) {
            prefixOutput.Write(Blocks_[i].Begin(), Blocks_[i].Size());
        }
        prefixOutput.Write(Blocks_[endBlockIndex].Begin(), endPtr - Blocks_[endBlockIndex].Begin());

        Blocks_.erase(Blocks_.begin(), Blocks_.begin() + endBlockIndex);
        result = prefixOutput.Flush();
    }

    PrefixStart_ = endPtr;

    YCHECK(PrefixStart_ >= Blocks_[0].begin());
    YCHECK(PrefixStart_ <= Blocks_[0].end());

    return result;
}

TSharedRef TStreamReader::ExtractPrefix(const char* endPtr)
{
    if (Blocks_.empty()) {
        return TSharedRef();
    }

    for (int i = 0; i < Blocks_.size(); ++i) {
        if (endPtr >= Blocks_[i].Begin() && endPtr <= Blocks_[i].End()) {
            return ExtractPrefix(i, endPtr);
        }
    }

    Y_UNREACHABLE();
}

TSharedRef TStreamReader::ExtractPrefix()
{
    return ExtractPrefix(CurrentPtr_);
}

TSharedRef TStreamReader::ExtractPrefix(size_t length)
{
    if (Blocks_.empty()) {
        return TSharedRef();
    }

    auto firstBlockSuffixLength = Blocks_[0].End() - PrefixStart_;
    if (length <= firstBlockSuffixLength) {
        return ExtractPrefix(0, PrefixStart_ + length);
    }

    length -= firstBlockSuffixLength;
    auto blockIndex = length / BlockSize_ + 1;
    YCHECK(blockIndex < Blocks_.size());
    auto positionInBlock = length % BlockSize_;
    return ExtractPrefix(blockIndex, Blocks_[blockIndex].Begin() + positionInBlock);
}

void TStreamReader::ReadNextBlock()
{
    auto block = TSharedMutableRef::Allocate<TInputStreamBlobTag>(BlockSize_, false);
    NextBlockSize_ = Stream_->Load(block.Begin(), block.Size());
    NextBlock_ = block;
    if (NextBlockSize_ == 0) {
        Finished_ = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
