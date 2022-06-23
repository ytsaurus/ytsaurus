#include "bus.h"

#include <yt/yt/core/net/connection.h>

namespace NYT::NZookeeper {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MessageLengthByteSize = 4;
constexpr i64 MaxMessageSize = 128_MB;

////////////////////////////////////////////////////////////////////////////////

void ValidateMessageLength(i64 length)
{
    if (length < 0) {
        THROW_ERROR_EXCEPTION("Zookeeper message length is negative")
            << TErrorAttribute("message_length", length);
    }
    if (length > MaxMessageSize) {
        THROW_ERROR_EXCEPTION("Zookeeper message is too long")
            << TErrorAttribute("message_length", length)
            << TErrorAttribute("max_messsage_length", MaxMessageSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef ReceiveMessage(const NNet::IConnectionReaderPtr& reader)
{
    struct TZookeeperMessageReaderTag
    { };

    constexpr i64 MessageLengthByteSize = 4;

    i64 messageLengthBytesToRead = MessageLengthByteSize;
    i64 messageBytesToRead = 0;

    std::vector<TSharedRef> requestParts;

    while (messageLengthBytesToRead > 0 || messageBytesToRead > 0) {
        auto bufferSize = messageLengthBytesToRead == 0 ? messageBytesToRead : messageLengthBytesToRead;
        auto buffer = TSharedMutableRef::Allocate<TZookeeperMessageReaderTag>(bufferSize);

        auto bytesRead = static_cast<i64>(WaitFor(reader->Read(buffer))
            .ValueOrThrow());
        if (bytesRead == 0) {
            THROW_ERROR_EXCEPTION("Premature end of the Zookeeper connection")
                << TErrorAttribute("request_length_bytes_to_read", messageLengthBytesToRead)
                << TErrorAttribute("request_bytes_to_read", messageBytesToRead);
        }

        auto part = buffer.Slice(0, bytesRead);
        if (messageLengthBytesToRead == 0) {
            YT_VERIFY(std::ssize(part) <= messageBytesToRead);
            requestParts.push_back(std::move(part));
            messageBytesToRead -= part.size();
        } else {
            YT_VERIFY(std::ssize(part) <= messageLengthBytesToRead);
            for (int index = 0; index < std::ssize(part); ++index) {
                messageBytesToRead = (messageBytesToRead << 8) | part[index];
            }

            messageLengthBytesToRead -= std::ssize(part);

            if (messageLengthBytesToRead == 0) {
                ValidateMessageLength(messageBytesToRead);
            }
        }
    }

    if (std::ssize(requestParts) == 1) {
        return requestParts[0];
    } else {
        return MergeRefsToRef<TZookeeperMessageReaderTag>(std::move(requestParts));
    }
}

void PostMessage(
    TSharedRef message,
    const NNet::IConnectionWriterPtr& writer)
{
    struct TZookeeperRequestWriterTag
    { };

    ValidateMessageLength(std::ssize(message));

    auto buffer = TSharedMutableRef::Allocate<TZookeeperRequestWriterTag>(MessageLengthByteSize);
    {
        int messageSize = message.size();
        for (int index = MessageLengthByteSize - 1; index >= 0; --index) {
            buffer[index] = messageSize & 255;
            messageSize >>= 8;
        }
    }

    TSharedRefArray parts(
        std::vector<TSharedRef>({buffer, message}),
        TSharedRefArray::TMoveParts{});
    WaitFor(writer->WriteV(parts))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
