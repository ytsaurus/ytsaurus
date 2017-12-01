#include "buffered_stream.h"
#include "helpers.h"

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

TBufferedStream::TBufferedStream(size_t capacity)
    : Data_(TSharedMutableRef::Allocate(capacity, false))
    , Begin_(Data_.Begin())
    , Capacity_(capacity)
    , AllowRead_(NewPromise<void>())
    , AllowWrite_(NewPromise<void>())
{ }

size_t TBufferedStream::WaitDataToRead(size_t size)
{
    TGuard<TMutex> guard(ReadMutex_);

    bool wait = false;

    {
        TGuard<TMutex> guard(Mutex_);

        if (Size_ < size) {
            SizeToRead_ = size;
            Capacity_ = std::max(Capacity_, size * 2);
            if (Full_) {
                Full_ = false;
                AllowWrite_.Set(TError());
            }
            if (!Finished_) {
                wait = true;
                AllowRead_ = NewPromise<void>();
            }
        }
    }

    if (wait) {
        // Busy wait.
        auto result = WaitForSettingFuture(AllowRead_.ToFuture());
        if (!result) {
            return 0;
        }
    }

    {
        TGuard<TMutex> guard(Mutex_);
        return std::min(size, Size_);
    }
}

void TBufferedStream::Read(size_t size, char* dest)
{
    TGuard<TMutex> guard(ReadMutex_);

    YCHECK(Size_ >= size);

    SizeToRead_ = 0;

    memcpy(dest, Begin_, size);
    Begin_ += size;
    Size_ -= size;

    if (Size_ * 2 < Capacity_ && Full_) {
        Full_ = false;
        AllowWrite_.Set(TError());
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

    AllowRead_.TrySet(TError());
}

TFuture<void> TBufferedStream::Close()
{
    Finish();
    return VoidFuture;
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
        AllowRead_.TrySet(TError());
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

////////////////////////////////////////////////////////////////////////////////

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

    // Shrink size to available data size if stream has finished.
    {
        TReleaseAcquireGilGuard guard;
        size = Stream_->WaitDataToRead(size);
    }

    if (PyErr_Occurred()) {
        throw Py::Exception();
    }

#if PY_MAJOR_VERSION >= 3
    auto* rawResult = PyBytes_FromStringAndSize(nullptr, size);
    char* underlyingString = PyBytes_AsString(rawResult);
#else
    auto* rawResult = PyString_FromStringAndSize(nullptr, size);
    char* underlyingString = PyBytes_AsString(rawResult);
#endif

    {
        TReleaseAcquireGilGuard guard;
        Stream_->Read(size, underlyingString);
    }

    return Py::Object(rawResult, true);
}

Py::Object TBufferedStreamWrap::Empty(Py::Tuple& args, Py::Dict& kwargs)
{
    ValidateArgumentsEmpty(args, kwargs);

    bool empty;
    {
        TReleaseAcquireGilGuard guard;
        empty = Stream_->Empty();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
