#include "helpers.h"
#include "buffered_stream.h"

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

TBufferedStream::TBufferedStream(size_t bufferSize)
    : Size_(0)
    , AllowedSize_(bufferSize / 2)
    , Data_(TSharedMutableRef::Allocate(bufferSize, false))
    , Begin_(Data_.Begin())
    , End_(Data_.Begin())
    , State_(EState::Normal)
    , AllowWrite_(NewPromise<void>())
    , AllowRead_(NewPromise<void>())
{ }

TSharedRef TBufferedStream::Read(size_t size)
{
    YCHECK(State_ != EState::WaitingData);

    if (Size_ >= size) {
        return ExtractChunk(size);
    }

    bool wait = false;
    {
        TGuard<TMutex> guard(Mutex_);
        if (State_ == EState::Full) {
            AllowedSize_ = std::max(AllowedSize_, size);
            AllowWrite_.Set(TError());
        }

        if (State_ != EState::Finished)
        {
            wait = true;
            State_ = EState::WaitingData;
            AllowRead_ = NewPromise<void>();
        }
    }

    if (wait) {
        AllowRead_.Get();
    }

    return ExtractChunk(size);
}

bool TBufferedStream::Empty() const
{
    return Size_ == 0;
}

void TBufferedStream::Finish()
{
    TGuard<TMutex> guard(Mutex_);

    YASSERT(State_ != EState::Finished);

    if (State_ == EState::WaitingData) {
        AllowRead_.Set();
    }
    if (State_ == EState::Full) {
        AllowWrite_.Set(TError());
    }

    State_ = EState::Finished;
}

TFuture<void> TBufferedStream::Write(const TSharedRef& buffer)
{
    YCHECK(State_ != EState::Full);

    {
        TGuard<TMutex> guard(Mutex_);

        if (Data_.End() - End_ < buffer.Size()) {
            if (Size_ + buffer.Size() > Data_.Size()) {
                Reallocate(std::max(Size_ + buffer.Size(), Data_.Size() * 2));
            } else if (End_ - Begin_ <= Begin_ - Data_.Begin()) {
                Move(Data_.Begin());
            } else {
                Reallocate(Data_.Size());
            }
        }

        std::copy(buffer.Begin(), buffer.Begin() + buffer.Size(), End_);
        End_ = End_ + buffer.Size();
        Size_ += buffer.Size();
    }

    if (Size_ >= AllowedSize_) {
        TGuard<TMutex> guard(Mutex_);

        if (State_ == EState::WaitingData) {
            AllowRead_.Set();
        }

        AllowWrite_ = NewPromise<void>();
        State_ = EState::Full;

        return AllowWrite_;
    } else {
        return VoidFuture;
    }
}

void TBufferedStream::Reallocate(size_t len)
{
    auto newData = TSharedMutableRef::Allocate(len, false);
    Move(newData.Begin());
    std::swap(Data_, newData);
}

void TBufferedStream::Move(char* dest)
{
    std::copy(Begin_, End_, dest);
    End_ = dest + (End_ - Begin_);
    Begin_ = dest;
}

TSharedRef TBufferedStream::ExtractChunk(size_t size)
{
    TGuard<TMutex> guard(Mutex_);

    size = std::min(size, static_cast<size_t>(End_ - Begin_));

    auto result = Data_.Slice(Begin_, Begin_ + size);
    Begin_ += size;

    Size_ -= size;
    if (Size_ < AllowedSize_ && State_ == EState::Full) {
        AllowWrite_.Set(TError());
        State_ = EState::Normal;
    }

    return result;
}

///////////////////////////////////////////////////////////////////////////////

TBufferedStreamWrap::TBufferedStreamWrap(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TBufferedStreamWrap>::PythonClass(self, args, kwargs)
    , Stream_(New<TBufferedStream>(Py::Int(ExtractArgument(args, kwargs, "size")).asLongLong()))
{
    ValidateArgumentsEmpty(args, kwargs);
}

Py::Object TBufferedStreamWrap::Read(Py::Tuple& args, Py::Dict& kwargs)
{
    auto size = Py::Int(ExtractArgument(args, kwargs, "size"));
    ValidateArgumentsEmpty(args, kwargs);

    TSharedRef result;
    {
        Py_BEGIN_ALLOW_THREADS
        result = Stream_->Read(size.asLongLong());
        Py_END_ALLOW_THREADS
    }
    return Py::String(result.Begin(), result.Size());
}

Py::Object TBufferedStreamWrap::Empty(Py::Tuple& args, Py::Dict& kwargs)
{
    ValidateArgumentsEmpty(args, kwargs);
    return Py::Boolean(Stream_->Empty());
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
