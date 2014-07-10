#include "public.h"

#include <core/concurrency/async_stream.h>

#include <ytlib/driver/driver.h>

#include <util/system/mutex.h>

#include <contrib/libs/pycxx/Extensions.hxx>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TBufferedStream
    : public NConcurrency::IAsyncOutputStream
{
public:
    explicit TBufferedStream(size_t bufferSize);

    TSharedRef Read(size_t size = 0);

    bool Empty() const;

    void Finish(NDriver::TDriverResponse);

    virtual TAsyncError Write(const void* buf, size_t len) override;

private:
    size_t Size_;
    size_t AllowedSize_;

    TSharedRef Data_;
    char* Begin_;
    char* End_;

    DECLARE_ENUM(EState,
        (Normal)
        (Full)
        (WaitingData)
        (Finished)
    );

    EState State_;

    TAsyncErrorPromise AllowWrite_;
    TPromise<void> AllowRead_;

    TMutex Mutex_;

    void Reallocate(size_t len);
    void Move(char* dest);
    TSharedRef ExtractChunk(size_t size);
};

///////////////////////////////////////////////////////////////////////////////

class TBufferedStreamWrap
    : public Py::PythonClass<TBufferedStreamWrap>
{
public:
    TBufferedStreamWrap(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    Py::Object Read(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TBufferedStreamWrap, Read);

    Py::Object Empty(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TBufferedStreamWrap, Empty);

    TBufferedStreamPtr GetStream();

    virtual ~TBufferedStreamWrap();

    static void InitType();

private:
    TBufferedStreamPtr Stream_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
