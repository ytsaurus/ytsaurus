#pragma once

#include <util/generic/buffer.h>
#include <util/stream/output.h>

namespace NSkiff {
    class TUncheckedSkiffWriter
        : private IOutputStream
    {
    public:
        explicit TUncheckedSkiffWriter(IOutputStream* underlying);

        ~TUncheckedSkiffWriter();

        void WriteDouble(double value);
        void WriteBoolean(bool value);

        void WriteInt64(i64 value);
        void WriteUint64(ui64 value);

        void WriteString32(TStringBuf value);

        void WriteYson32(TStringBuf value);

        void WriteVariant8Tag(ui8 tag);
        void WriteVariant16Tag(ui16 tag);

        using IOutputStream::Flush;
        void Finish();

        void SetBufferCapacity(size_t s);
    private:
        void DoWrite(const void* data, size_t size) override final;
        void DoFlush() override final;

        template <typename T>
        void WriteSimple(T data);

    private:
        IOutputStream* const Underlying_;
        TBuffer Buffer_;
        size_t RemainingBytes_;
        char* Position_;
    };
}
