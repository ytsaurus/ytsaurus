#include "packer.h"

#include <library/cpp/framing/packer.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

std::string PackBatch(
    const std::vector<TOutputMessageConstPtr>& messages,
    const std::string& dataColumn,
    const EFramingFormat framingFormat)
{
    TStringStream stream;
    NFraming::TPacker packer(static_cast<NFraming::EFormat>(framingFormat), stream);
    for (const auto& message : messages) {
        const auto& data = GetColumnValue<TStringBuf>(*message, dataColumn);
        packer.Add(data);
    }
    packer.Flush();
    return stream.Str();
}

std::vector<std::pair<std::string, std::vector<TOutputMessageConstPtr>>> PackBatches(
    const std::deque<TOutputMessageConstPtr>& messages,
    const std::string& dataColumn,
    const EFramingFormat framingFormat,
    const i32 maxRowsPerMessage,
    const i32 maxBytesPerMessage)
{
    i64 batchByteSize = 0;
    std::vector<TOutputMessageConstPtr> batch;
    std::vector<std::pair<std::string, std::vector<TOutputMessageConstPtr>>> batchedMessages;

    auto flush = [&] {
        if (batch.empty()) {
            return;
        }

        TStringStream stream;
        NFraming::TPacker packer(static_cast<NFraming::EFormat>(framingFormat), stream);
        for (const auto& message : batch) {
            const auto& data = GetColumnValue<TStringBuf>(*message, dataColumn);
            packer.Add(data);
        }
        packer.Flush();

        if (stream.Empty()) {
            return;
        }

        batchedMessages.emplace_back(std::move(stream).Str(), std::move(batch));
        batchByteSize = 0;
        batch.clear();
    };

    for (const auto& message : messages) {
        batch.push_back(message);
        batchByteSize += message->ByteSize;
        if (batchByteSize >= maxBytesPerMessage || std::ssize(batch) >= maxRowsPerMessage) {
            flush();
        }
    }

    flush();

    return batchedMessages;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
