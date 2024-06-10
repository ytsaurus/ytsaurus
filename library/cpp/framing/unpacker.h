#pragma once

#include "format.h"

#include <util/generic/strbuf.h>

namespace google::protobuf {
    class Message;
}

namespace NFraming {

    // @brief class to iterate over frames in original data
    class TUnpacker {
    public:
        TUnpacker(EFormat format, TStringBuf data);

        TUnpacker(TStringBuf data) : TUnpacker(EFormat::Protoseq, data) {}

        //@brief returns actual format of frame (usefull when unpacker is created with format Auto)
        EFormat Format() const noexcept {
            return Format_;
        }

        // @brief part of input which is not processed yet
        TStringBuf Tail() const noexcept {
            return Data_;
        }

        // @brief extracts next frame from data
        // @brief frame will contain part of data which frame if it was found
        // @param skipped will contain part of original data which was skipped
        // @return true if next frame was found, otherwise false
        bool NextFrame(TStringBuf& frame, TStringBuf& skipped) noexcept;

        // @brief extracts next frame as protobuf
        // @note ignores data which cannot be decoded as protobuf
        bool NextFrame(google::protobuf::Message& frame, TStringBuf& skipped);
    private:
        TStringBuf Data_;
        EFormat Format_;
    };

    // @brief unpack from string
    // @brief frame will contain part of data which frame if it was found
    // @param skipped will contain part of original data which was skipped
    // @param return size of consumed part on success otherwise npos
    size_t UnpackFromString(EFormat format, TStringBuf data, TStringBuf& frame, TStringBuf& skipped);
}
