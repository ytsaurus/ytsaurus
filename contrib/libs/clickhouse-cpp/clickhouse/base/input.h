#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <memory>

namespace clickhouse {

class InputStream {
public:
    virtual ~InputStream() noexcept (false)
    { }

    /// Reads one byte from the stream.
    inline bool ReadByte(uint8_t* byte) {
        return DoRead(byte, sizeof(uint8_t)) == sizeof(uint8_t);
    }

    /// Reads some data from the stream.
    inline size_t Read(void* buf, size_t len) {
        return DoRead(buf, len);
    }

    // Skips a number of bytes.  Returns false if an underlying read error occurs.
    virtual bool Skip(size_t bytes) = 0;

protected:
    virtual size_t DoRead(void* buf, size_t len) = 0;
};


class ZeroCopyInput : public InputStream {
public:
    inline size_t Next(const void** buf, size_t len) {
        return DoNext(buf, len);
    }

    bool Skip(size_t bytes) override;

protected:
    virtual size_t DoNext(const void** ptr, size_t len) = 0;

    size_t DoRead(void* buf, size_t len) override;
};


/**
 * A ZeroCopyInput stream backed by an in-memory array of bytes.
 */
class ArrayInput : public ZeroCopyInput {
public:
     ArrayInput() noexcept;
     ArrayInput(const void* buf, size_t len) noexcept;
    ~ArrayInput() override;

    /// Number of bytes available in the stream.
    inline size_t Avail() const noexcept {
        return len_;
    }

    /// Current read position in the memory block used by this stream.
    inline const uint8_t* Data() const noexcept {
        return data_;
    }

    /// Whether there is more data in the stream.
    inline bool Exhausted() const noexcept {
        return !Avail();
    }

    inline void Reset(const void* buf, size_t len) noexcept {
        data_ = static_cast<const uint8_t*>(buf);
        len_ = len;
    }

private:
    size_t DoNext(const void** ptr, size_t len) override;

private:
    const uint8_t* data_;
    size_t len_;
};


class BufferedInput : public ZeroCopyInput {
public:
    BufferedInput(std::unique_ptr<InputStream> source, size_t buflen = 8192);
    ~BufferedInput() override;

    void Reset();

protected:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoNext(const void** ptr, size_t len) override;

private:
    std::unique_ptr<InputStream> const source_;
    ArrayInput array_input_;
    std::vector<uint8_t> buffer_;
};

}
