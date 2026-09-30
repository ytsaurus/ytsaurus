#include "output.h"

#include <algorithm>
#include <assert.h>
#include <memory.h>

namespace clickhouse {

size_t ZeroCopyOutput::DoWrite(const void* data, size_t len) {
    const size_t original_len = len;
    while (len > 0) {
        void* ptr;
        size_t result = DoNext(&ptr, len);

        if (result) {
            memcpy(ptr, data, result);
            len -= result;
            data = static_cast<const uint8_t*>(data) + result;
        } else {
            break;
        }
    }

    return original_len - len;
}


ArrayOutput::ArrayOutput(void* buf, size_t len)
    : buf_(static_cast<uint8_t*>(buf))
    , end_(buf_ + len)
    , buffer_size_(len)
{
}

ArrayOutput::~ArrayOutput() = default;

size_t ArrayOutput::DoNext(void** data, size_t len) {
    len = std::min(len, Avail());

    *data = buf_;
    buf_ += len;

    return len;
}


BufferOutput::BufferOutput(Buffer* buf)
    : buf_(buf)
    , pos_(0)
{
    assert(buf_);
}

BufferOutput::~BufferOutput()
{ }

size_t BufferOutput::DoNext(void** data, size_t len) {
    if (pos_ + len > buf_->size()) {
        buf_->resize(pos_ + len);
    }

    *data = buf_->data() + pos_;
    pos_ += len;

    return len;
}


BufferedOutput::BufferedOutput(std::unique_ptr<OutputStream> destination, size_t buflen)
    : destination_(std::move(destination))
    , buffer_(buflen)
    , array_output_(buffer_.data(), buflen)
{
}

BufferedOutput::~BufferedOutput() { }

void BufferedOutput::Reset() {
    array_output_.Reset(buffer_.data(), buffer_.size());
}

void BufferedOutput::DoFlush() {
    if (array_output_.Data() != buffer_.data()) {
        size_t len = array_output_.Data() - buffer_.data();
        const uint8_t* buf = buffer_.data();
        while (len > 0) {
            const size_t written = destination_->Write(buf, len);
            buf += written;
            len -= written;
        }

        destination_->Flush();

        array_output_.Reset(buffer_.data(), buffer_.size());
    }
}

size_t BufferedOutput::DoNext(void** data, size_t len) {
    if (array_output_.Avail() < len) {
        Flush();
    }

    return array_output_.Next(data, len);

}

size_t BufferedOutput::DoWrite(const void* data, size_t len) {
    if (array_output_.Avail() < len) {
        Flush();

        if (len > buffer_.size() / 2) {
            return destination_->Write(data, len);
        }
    }

    return array_output_.Write(data, len);
}

}
