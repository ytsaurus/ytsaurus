#pragma once

#ifndef UNPACKER_INL_H_
    #error "Direct inclusion of this file is not allowed, include unpacker.h"
    // For the sake of sane code completion.
    #include "unpacker.h"
#endif

#include <library/cpp/framing/unpacker.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
TErrorOr<std::vector<TProto>> UnpackBatch(
    TStringBuf rawData,
    const EFramingFormat framingFormat)
{
    std::vector<TProto> result;
    try {
        auto unpacker = NFraming::TUnpacker(static_cast<NFraming::EFormat>(framingFormat), rawData);
        TProto proto;
        TStringBuf skipped;
        while (unpacker.NextFrame(proto, skipped)) {
            if (!skipped.empty()) {
                THROW_ERROR_EXCEPTION("Error during decompression of framed messages, skipped data: %Qv",
                    skipped)
                    .With("is_permanent", true);
            }
            result.push_back(std::move(proto));
            // NOLINTNEXTLINE(bugprone-use-after-move)
            proto.Clear();
        }
        if (!skipped.empty()) {
            THROW_ERROR_EXCEPTION("Error during decompression of framed messages, skipped data: %Qv",
                skipped)
                .With("is_permanent", true);
        }

        return result;
    } catch (const std::exception& ex) {
        return TError(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
