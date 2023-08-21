#include "tee_input_stream.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

TTeeInputStream::TTeeInputStream(IZeroCopyInput* underlying)
    : Underlying_(underlying)
{ }

TTeeInputStream::~TTeeInputStream() = default;

size_t TTeeInputStream::Size() const
{
    return RingBuffer_.Size();
}

void TTeeInputStream::ExtractFromBuffer(TBuffer* destination, size_t count)
{
    RingBuffer_.Pop(destination, count);
}

void TTeeInputStream::Flush()
{
    RingBuffer_.Clear();
}

size_t TTeeInputStream::DoNext(const void** ptr, size_t len)
{
    size_t read = Underlying_->Next(ptr, len);
    RingBuffer_.Push(TStringBuf(static_cast<const char*>(*ptr), read));
    return read;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
