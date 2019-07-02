#include "unchecked_writer.h"

#include <util/system/unaligned_mem.h>

namespace NSkiff {
    TUncheckedSkiffWriter::TUncheckedSkiffWriter(IOutputStream* underlying)
        : Underlying_(underlying)
        , Buffer_(1 * 1024)
        , RemainingBytes_(Buffer_.Capacity())
        , Position_(Buffer_.Data())
    {
    }

    TUncheckedSkiffWriter::~TUncheckedSkiffWriter() {
        try {
            DoFlush();
        } catch (...) {
        }
    }

    template <typename T>
    void TUncheckedSkiffWriter::WriteSimple(T value) {
        if (RemainingBytes_ < sizeof(T)) {
            DoFlush();
            Y_ASSERT(RemainingBytes_ >= sizeof(T));
        }
        WriteUnaligned<T>(Position_, value);
        Position_ += sizeof(T);
        RemainingBytes_ -= sizeof(T);
    }

    void TUncheckedSkiffWriter::WriteInt64(i64 value) {
        WriteSimple<i64>(value);
    }

    void TUncheckedSkiffWriter::WriteUint64(ui64 value) {
        WriteSimple<ui64>(value);
    }

    void TUncheckedSkiffWriter::WriteDouble(double value) {
        return WriteSimple<double>(value);
    }

    void TUncheckedSkiffWriter::WriteBoolean(bool value) {
        return WriteSimple<ui8>(value ? 1 : 0);
    }

    void TUncheckedSkiffWriter::WriteString32(TStringBuf value) {
        WriteSimple<ui32>(value.size());
        TUncheckedSkiffWriter::DoWrite(value.data(), value.size());
    }

    void TUncheckedSkiffWriter::WriteYson32(TStringBuf value) {
        WriteSimple<ui32>(value.size());
        TUncheckedSkiffWriter::DoWrite(value.data(), value.size());
    }

    void TUncheckedSkiffWriter::WriteVariant8Tag(ui8 tag) {
        WriteSimple<ui8>(tag);
    }

    void TUncheckedSkiffWriter::WriteVariant16Tag(ui16 tag) {
        WriteSimple<ui16>(tag);
    }

    void TUncheckedSkiffWriter::DoFlush() {
        Underlying_->Write(Buffer_.Data(), Position_ - Buffer_.Data());
        Position_ = Buffer_.Data();
        RemainingBytes_ = Buffer_.Capacity();
    }

    void TUncheckedSkiffWriter::DoWrite(const void* data, size_t size) {
        if (size > RemainingBytes_) {
            DoFlush();
            if (size >= RemainingBytes_) {
                Underlying_->Write(data, size);
                return;
            }
        }
        memcpy(Position_, data, size);
        Position_ += size;
        RemainingBytes_ -= size;
    }

    void TUncheckedSkiffWriter::Finish() {
        IOutputStream::Flush();
    }

    void TUncheckedSkiffWriter::SetBufferCapacity(size_t s) {
        size_t positionShift = Position_ - Buffer_.Data();
        Buffer_.Reserve(s);
        Position_ = Buffer_.Data() + positionShift;
    }
}
