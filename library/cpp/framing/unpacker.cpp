#include "unpacker.h"
#include "syncword.h"
#include "utils.h"

#include <google/protobuf/message.h>
#include <google/protobuf/io/coded_stream.h>

#include <util/system/byteorder.h>

namespace {
    using namespace NFraming::NPrivate;
    using NProtoBuf::io::CodedInputStream;

    bool TryUnpackProtoseqFrame(TStringBuf& buf, TStringBuf& data) noexcept {
        if (buf.size() >= sizeof(ui32)) {
            ui32 dataLen = LittleToHost(*(const ui32*)buf.data());
            constexpr size_t dataOffset = PROTOSEQ_FRAME_LEN_BYTES;
            const size_t frameLen = GetProtoseqFrameSize(dataLen);
            if (buf.size() >= frameLen && buf.SubStr(dataOffset + dataLen, SyncWord.size()) == SyncWord) {
                data = buf.SubStr(dataOffset, dataLen);
                buf.Skip(frameLen);
                return true;
            }
        }
        return false;
    }

    bool TryUnpackLenvalFrame(TStringBuf& buf, TStringBuf& data) noexcept {
        CodedInputStream stream(reinterpret_cast<const ui8*>(buf.data()), buf.size());
        ui64 dataLen;
        if (stream.ReadVarint64(&dataLen)) {
            const size_t dataOffset = stream.CurrentPosition();
            if (dataOffset + dataLen <= buf.size()) {
                data = buf.SubStr(dataOffset, dataLen);
                buf.Skip(dataOffset + dataLen);
                return true;
            }
        }
        return false;
    }

    bool TryUnpackLightProtoseqFrame(TStringBuf& buf, TStringBuf& data) noexcept {
        if (buf.size() >= sizeof(ui32)) {
            ui32 dataLen = LittleToHost(*(const ui32*)buf.data());
            constexpr size_t dataOffset = PROTOSEQ_FRAME_LEN_BYTES;
            const size_t frameLen = GetLightProtoseqFrameSize(dataLen);
            if (buf.size() >= frameLen) {
                data = buf.SubStr(dataOffset, dataLen);
                buf.Skip(frameLen);
                return true;
            }
        }
        return false;
    }
}

namespace NFraming {
    TUnpacker::TUnpacker(EFormat format, TStringBuf data)
        : Data_(data)
        , Format_(format)
    {
        if (EFormat::Auto == Format_ && !Data_.empty()) {
            TStringBuf tmp;
            TStringBuf dataCopy = Data_;
            if (TryUnpackProtoseqFrame(dataCopy, tmp)) {
                Format_ = EFormat::Protoseq;
            } else {
                Format_ = EFormat::Lenval;
            }
        }
    }

    bool TUnpacker::NextFrame(TStringBuf& frame, TStringBuf& skipped) noexcept {
        skipped.Clear();
        frame.Clear();

        if (Data_.empty()) {
            return false;
        }

        if (EFormat::Lenval == Format_) {
            if (TryUnpackLenvalFrame(Data_, frame)) {
                return true;
            }
            // no recovery for lenval, so skip whole data
            skipped.swap(Data_);
            return false;
        }

        if (EFormat::LightProtoseq == Format_) {
            if (TryUnpackLightProtoseqFrame(Data_, frame)) {
                return true;
            }
            // no recovery for light protoseq, so skip whole data
            skipped.swap(Data_);
            return false;
        }

        // EFormat::Protoseq format
        if (TryUnpackProtoseqFrame(Data_, frame)) {
            return true;
        }

        TStringBuf data = Data_;
        // find end of corrupted frame
        while (true) {
            size_t signStart = data.find(SyncWord);
            if (TStringBuf::npos == signStart) {
                break;
            }

            data.Skip(signStart + SyncWord.size());
            if (TryUnpackProtoseqFrame(data, frame)) {
                skipped = Data_.substr(0, frame.data() - Data_.data() - PROTOSEQ_FRAME_LEN_BYTES);
                Data_.swap(data);
                return true;
            }
        }

        skipped.swap(Data_);
        return false;
    }

    bool TUnpacker::NextFrame(google::protobuf::Message& frame, TStringBuf& skipped) {
        TStringBuf raw;
        TStringBuf rawSkip;
        TStringBuf data = Data_;
        skipped.Clear();

        while (NextFrame(raw, rawSkip)) {
            if (frame.ParseFromArray(raw.data(), raw.size())) {
                skipped = data.SubStr(0, skipped.size() + rawSkip.size());
                return true;
            }
            skipped = data.SubStr(0, Data_.data() - data.data());
        }
        frame.Clear();
        Data_.Clear();
        skipped = data;
        return false;
    }

    size_t UnpackFromString(EFormat format, TStringBuf data, TStringBuf& frame, TStringBuf& skipped) {
        TUnpacker unpacker{format, data};
        if (unpacker.NextFrame(frame, skipped)) {
            return unpacker.Tail().data() - data.data();
        }
        return TStringBuf::npos;
    }
}
