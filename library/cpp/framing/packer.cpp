#include "packer.h"
#include "syncword.h"
#include "utils.h"

#include <google/protobuf/message.h>
#include <google/protobuf/io/coded_stream.h>

#include <util/generic/ylimits.h>
#include <util/generic/yexception.h>

namespace {
    using namespace NFraming::NPrivate;
    using google::protobuf::io::CodedOutputStream;

    inline ui8* WriteStringToArray(TStringBuf data, ui8* target) {
        return CodedOutputStream::WriteRawToArray(data.data(), static_cast<int>(data.size()), target);
    }

    size_t PackProtoseqFrameToArray(TStringBuf data, char* buffer) {
        Y_ENSURE(data.size() < Max<ui32>(), "length of frame should fit into 4 bytes");
        ui8* target = reinterpret_cast<ui8*>(buffer);
        ui8* cursor = target;
        cursor = CodedOutputStream::WriteLittleEndian32ToArray(static_cast<ui32>(data.size()), cursor);
        cursor = WriteStringToArray(data, cursor);
        cursor = WriteStringToArray(SyncWord, cursor);
        return cursor - target;
    }

    size_t PackLenvalFrameToArray(TStringBuf data, char* buffer) {
        ui8* target = reinterpret_cast<ui8*>(buffer);
        ui8* cursor = target;
        cursor = CodedOutputStream::WriteVarint64ToArray(data.size(), cursor);
        cursor = WriteStringToArray(data, cursor);
        return cursor - target;
    }

    size_t PackLightProtoseqFrameToArray(TStringBuf data, char* buffer) {
        Y_ENSURE(data.size() < Max<ui32>(), "length of frame should fit into 4 bytes");
        ui8* target = reinterpret_cast<ui8*>(buffer);
        ui8* cursor = target;
        cursor = CodedOutputStream::WriteLittleEndian32ToArray(static_cast<ui32>(data.size()), cursor);
        cursor = WriteStringToArray(data, cursor);
        return cursor - target;
    }

    size_t PackProtoseqFrameToArray(const google::protobuf::Message& message, char* buffer) {
        ui32 messageSize = message.GetCachedSize();
        ui8* target = reinterpret_cast<ui8*>(buffer);
        ui8* cursor = target;
        cursor = CodedOutputStream::WriteLittleEndian32ToArray(messageSize, cursor);
        cursor = message.SerializeWithCachedSizesToArray(cursor);
        cursor = WriteStringToArray(SyncWord, cursor);
        return cursor - target;
    }

    size_t PackLenvalFrameToArray(const google::protobuf::Message& message, char* buffer) {
        ui64 messageSize = message.GetCachedSize();
        ui8* target = reinterpret_cast<ui8*>(buffer);
        ui8* cursor = target;
        cursor = CodedOutputStream::WriteVarint64ToArray(messageSize, cursor);
        cursor = message.SerializeWithCachedSizesToArray(cursor);
        return cursor - target;
    }

    size_t PackLightProtoseqFrameToArray(const google::protobuf::Message& message, char* buffer) {
        ui32 messageSize = message.GetCachedSize();
        ui8* target = reinterpret_cast<ui8*>(buffer);
        ui8* cursor = target;
        cursor = CodedOutputStream::WriteLittleEndian32ToArray(messageSize, cursor);
        cursor = message.SerializeWithCachedSizesToArray(cursor);
        return cursor - target;
    }

    inline void DoResize(TBuffer& buffer, size_t size) {
        buffer.Resize(size);
    }

    inline void DoResize(TString& buffer, size_t size) {
        buffer.resize(size);
    }

    template <class I, class O>
    void DoPack(NFraming::EFormat format, const I& data, size_t dataSize, O& out) {
        switch (format) {
            case NFraming::EFormat::Auto:
            case NFraming::EFormat::Protoseq:
                DoResize(out, GetProtoseqFrameSize(dataSize));
                DoResize(out, PackProtoseqFrameToArray(data, const_cast<char*>(out.Data())));
                break;
            case NFraming::EFormat::Lenval:
                DoResize(out, GetLenvalFrameSize(dataSize));
                DoResize(out, PackLenvalFrameToArray(data, const_cast<char*>(out.Data())));
                break;
            case NFraming::EFormat::LightProtoseq:
                DoResize(out, GetLightProtoseqFrameSize(dataSize));
                DoResize(out, PackLightProtoseqFrameToArray(data, const_cast<char*>(out.Data())));
                break;
        }
    }
}

namespace NFraming {

    size_t TPacker::GetFrameSize(size_t messageSize) const {
        return NFraming::GetFrameSize(Format_, messageSize);
    }

    void TPacker::Add(TStringBuf data) {
        DoPack(Format_, data, data.size(), Buffer_);
        Out_->Write(Buffer_.Data(), Buffer_.Size());
    }

    void TPacker::Add(const google::protobuf::Message& message, bool useCachedSize) {
        const size_t messageSize = useCachedSize ? message.GetCachedSize() : message.ByteSize();
        DoPack(Format_, message, messageSize, Buffer_);
        Out_->Write(Buffer_.Data(), Buffer_.Size());
    }

    void TPacker::Flush() {
        Out_->Flush();
    }

    TString PackToString(EFormat format, TStringBuf data) {
        TString result;
        DoPack(format, data, data.size(), result);
        return result;
    }

    TString PackToString(EFormat format, const google::protobuf::Message& message, const bool useCachedSize) {
        TString result;
        const size_t messageSize = useCachedSize ? message.GetCachedSize() : message.ByteSize();
        DoPack(format, message, messageSize, result);
        return result;
    }
}
