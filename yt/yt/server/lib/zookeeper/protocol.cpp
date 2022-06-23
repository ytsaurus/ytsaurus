#include "protocol.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

class TZookeeperProtocolReader
    : public IZookeeperProtocolReader
{
public:
    explicit TZookeeperProtocolReader(TSharedRef data)
        : Data_(std::move(data))
    { }

    char ReadByte() override
    {
        return DoReadLong(/*byteSize*/ 1);
    }

    int ReadInt() override
    {
        return DoReadLong(/*byteSize*/ 4);
    }

    i64 ReadLong() override
    {
        return DoReadLong(/*byteSize*/ 8);
    }

    bool ReadBool() override
    {
        auto value = ReadByte();
        return value > 0;
    }

    TString ReadString() override
    {
        auto length = ReadInt();
        ValidateSizeAvailable(length);

        TString result;
        result.resize(length);
        auto begin = Data_.begin() + Offset_;
        std::copy(begin, begin + length, result.begin());
        Offset_ += length;

        return result;
    }

    bool IsFinished() const override
    {
        return Offset_ == std::ssize(Data_);
    }

    void ValidateFinished() const override
    {
        if (!IsFinished()) {
            THROW_ERROR_EXCEPTION("Expected end of stream")
                << TErrorAttribute("offset", Offset_)
                << TErrorAttribute("message_size", Data_.size());
        }
    }

private:
    const TSharedRef Data_;
    i64 Offset_ = 0;

    i64 DoReadLong(int byteSize)
    {
        ValidateSizeAvailable(byteSize);

        int result = 0;
        for (int index = 0; index < byteSize; ++index) {
            result = (result << 8) | Data_[Offset_++];
        }

        return result;
    }

    void ValidateSizeAvailable(i64 size)
    {
        if (Offset_ + size > std::ssize(Data_)) {
            THROW_ERROR_EXCEPTION("Premature end of stream while reading %v bytes", size);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IZookeeperProtocolReader> CreateZookeeperProtocolReader(TSharedRef data)
{
    return std::make_unique<TZookeeperProtocolReader>(std::move(data));
}

////////////////////////////////////////////////////////////////////////////////

class TZookeeperProtocolWriter
    : public IZookeeperProtocolWriter
{
public:
    TZookeeperProtocolWriter()
        : Buffer_(AllocateBuffer(InitialBufferSize))
    { }

    void WriteByte(char value) override
    {
        DoWriteInt(value, /*byteSize*/ 1);
    }

    void WriteInt(int value) override
    {
        DoWriteInt(value, /*byteSize*/ 4);
    }

    void WriteLong(i64 value) override
    {
        DoWriteInt(value, /*byteSize*/ 8);
    }

    void WriteBool(bool value) override
    {
        WriteByte(value ? 1 : 0);
    }

    void WriteString(const TString& value) override
    {
        WriteInt(value.size());

        EnsureFreeSpace(value.size());

        std::copy(value.begin(), value.end(), Buffer_.begin() + Size_);
        Size_ += value.size();
    }

    TSharedRef Finish() override
    {
        return Buffer_.Slice(0, Size_);
    }

private:
    struct TZookeeperProtocolWriterTag
    { };

    constexpr static i64 InitialBufferSize = 16_KB;
    constexpr static i64 BufferSizeMultiplier = 2;

    TSharedMutableRef Buffer_;
    i64 Size_ = 0;

    void DoWriteInt(i64 value, int byteSize)
    {
        EnsureFreeSpace(byteSize);

        for (int index = byteSize - 1; index >= 0; --index) {
            Buffer_[Size_ + index] = value & 255;
            value >>= 8;
        }

        Size_ += byteSize;
    }

    void EnsureFreeSpace(i64 size)
    {
        if (Size_ + size <= std::ssize(Buffer_)) {
            return;
        }

        auto newSize = std::max<i64>(Size_ + size, Buffer_.size() * BufferSizeMultiplier);
        auto newBuffer = AllocateBuffer(newSize);
        std::copy(Buffer_.begin(), Buffer_.begin() + Size_, newBuffer.begin());
        Buffer_ = std::move(newBuffer);
    }

    static TSharedMutableRef AllocateBuffer(i64 capacity)
    {
        return TSharedMutableRef::Allocate<TZookeeperProtocolWriterTag>(capacity);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IZookeeperProtocolWriter> CreateZookeeperProtocolWriter()
{
    return std::make_unique<TZookeeperProtocolWriter>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
