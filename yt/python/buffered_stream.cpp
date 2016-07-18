#include "buffered_stream.h"
#include "helpers.h"

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

TBufferedStream::TBufferedStream(size_t capacity)
    : Data_(TSharedMutableRef::Allocate(capacity, false))
    , Begin_(Data_.Begin())
    , Capacity_(capacity)
    , AllowWrite_(NewPromise<void>())
{ }

PyObject* TBufferedStream::Read(size_t size)
{
    TGuard<TMutex> guard(ReadMutex_);

    bool wait = false;

    {
        TGuard<TMutex> guard(Mutex_);

        if (Size_ >= size) {
            return ExtractChunk(size);
        }

        SizeToRead_ = size;
        Capacity_ = std::max(Capacity_, size * 2);
        if (Full_) {
            Full_ = false;
            AllowWrite_.Set(TError());
        }
        if (!Finished_) {
            wait = true;
            AllowRead_.store(false);
        }
    }

    if (wait) {
        // Busy wait.
        while (true) {
            if (AllowRead_.load()) {
                break;
            }
        }
    }

    {
        TGuard<TMutex> guard(Mutex_);
        SizeToRead_ = 0;
        return ExtractChunk(size);
    }
}

bool TBufferedStream::Empty() const
{
    TGuard<TMutex> guard(Mutex_);
    return Finished_ && Size_ == 0;
}

void TBufferedStream::Finish()
{
    TGuard<TMutex> guard(Mutex_);

    YCHECK(!Finished_);

    Finished_ = true;

    AllowRead_.store(true);
}

TFuture<void> TBufferedStream::Write(const TSharedRef& data)
{
    TGuard<TMutex> guard(Mutex_);

    YCHECK(!Finished_);

    {
        if (Data_.End() < Begin_ + Size_ + data.Size()) {
            if (Size_ + data.Size() > Data_.Size()) {
                Reallocate(std::max(Size_ + data.Size(), Data_.Size() * 2));
            } else if (Size_ <= Begin_ - Data_.Begin()) {
                Move(Data_.Begin());
            } else {
                Reallocate(Data_.Size());
            }
        }

        std::copy(data.Begin(), data.Begin() + data.Size(), Begin_ + Size_);
        Size_ += data.Size();
    }

    if (Size_ >= SizeToRead_) {
        AllowRead_.store(true);
    }

    if (Capacity_ <= Size_ * 2) {
        Full_ = true;
        AllowWrite_ = NewPromise<void>();
        return AllowWrite_;
    } else {
        return VoidFuture;
    }
}

void TBufferedStream::Reallocate(size_t len)
{
    YCHECK(len >= Size_);

    auto newData = TSharedMutableRef::Allocate(len, false);
    Move(newData.Begin());
    std::swap(Data_, newData);
}

void TBufferedStream::Move(char* dest)
{
    std::copy(Begin_, Begin_ + Size_, dest);
    Begin_ = dest;
}

PyObject* TBufferedStream::ExtractChunk(size_t size)
{
    size = std::min(size, static_cast<size_t>(Size_));

    auto result = Data_.Slice(Begin_, Begin_ + size);
    Begin_ += size;
    Size_ -= size;

    if (Size_ * 2 < Capacity_ && Full_) {
        Full_ = false;
        AllowWrite_.Set(TError());
    }

    // NB: we could not call Py::Bytes since it calls PyBytes_Check
    // that requires GIL in python2.6.
    return PyBytes_FromStringAndSize(result.Begin(), static_cast<int>(result.Size()));
}

///////////////////////////////////////////////////////////////////////////////

TBufferedStreamWrap::TBufferedStreamWrap(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TBufferedStreamWrap>::PythonClass(self, args, kwargs)
    , Stream_(New<TBufferedStream>(Py::ConvertToLongLong(ExtractArgument(args, kwargs, "size"))))
{
    ValidateArgumentsEmpty(args, kwargs);
}

Py::Object TBufferedStreamWrap::Read(Py::Tuple& args, Py::Dict& kwargs)
{
    auto size = Py::ConvertToLongLong(ExtractArgument(args, kwargs, "size"));
    ValidateArgumentsEmpty(args, kwargs);

    PyObject* rawResult;
    {
        Py_BEGIN_ALLOW_THREADS
        rawResult = Stream_->Read(size);
        Py_END_ALLOW_THREADS
    }
    return Py::Object(rawResult, true);
}

Py::Object TBufferedStreamWrap::Empty(Py::Tuple& args, Py::Dict& kwargs)
{
    ValidateArgumentsEmpty(args, kwargs);

    bool empty;
    {
        Py_BEGIN_ALLOW_THREADS
        empty = Stream_->Empty();
        Py_END_ALLOW_THREADS
    }
    return Py::Boolean(empty);
}

TBufferedStreamPtr TBufferedStreamWrap::GetStream()
{
    return Stream_;
}

TBufferedStreamWrap::~TBufferedStreamWrap()
{ }

void TBufferedStreamWrap::InitType()
{
    behaviors().name("BufferedStream");
    behaviors().doc("Buffered stream to perform read and download asynchronously");
    behaviors().supportGetattro();
    behaviors().supportSetattro();

    PYCXX_ADD_KEYWORDS_METHOD(read, Read, "Synchronously read data from stream");
    PYCXX_ADD_KEYWORDS_METHOD(empty, Empty, "Check wether the stream is empty");

    behaviors().readyType();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
