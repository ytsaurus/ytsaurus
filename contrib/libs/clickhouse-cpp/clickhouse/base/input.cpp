#include "input.h"

#include <algorithm>
#include <memory.h>

namespace clickhouse {

bool ZeroCopyInput::Skip(size_t bytes) {
    while (bytes > 0) {
        const void* ptr;
        size_t len = Next(&ptr, bytes);

        if (len == 0) {
            return false;
        }

        bytes -= len;
    }

    return true;
}

size_t ZeroCopyInput::DoRead(void* buf, size_t len) {
    const void* ptr;
    size_t result = DoNext(&ptr, len);

    if (result) {
        memcpy(buf, ptr, result);
    }

    return result;
}

ArrayInput::ArrayInput() noexcept
    : data_(nullptr)
    , len_(0)
{
}

ArrayInput::ArrayInput(const void* buf, size_t len) noexcept
    : data_(static_cast<const uint8_t*>(buf))
    , len_(len)
{
}

ArrayInput::~ArrayInput() = default;

size_t ArrayInput::DoNext(const void** ptr, size_t len) {
    len = std::min(len_, len);

    *ptr   = data_;
    len_  -= len;
    data_ += len;

    return len;
}


BufferedInput::BufferedInput(std::unique_ptr<InputStream> source, size_t buflen)
    : source_(std::move(source))
    , array_input_(nullptr, 0)
    , buffer_(buflen)
{
}

BufferedInput::~BufferedInput() = default;

void BufferedInput::Reset() {
    array_input_.Reset(nullptr, 0);
}

size_t BufferedInput::DoNext(const void** ptr, size_t len)  {
    if (array_input_.Exhausted()) {
        array_input_.Reset(
            buffer_.data(), source_->Read(buffer_.data(), buffer_.size())
        );
    }

    return array_input_.Next(ptr, len);
}

size_t BufferedInput::DoRead(void* buf, size_t len) {
    if (array_input_.Exhausted()) {
        if (len > buffer_.size() / 2) {
            return source_->Read(buf, len);
        }

        array_input_.Reset(
            buffer_.data(), source_->Read(buffer_.data(), buffer_.size())
        );
    }

    return array_input_.Read(buf, len);
}

}
