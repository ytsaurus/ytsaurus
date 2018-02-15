#pragma once

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>

#include <util/stream/input.h>
#include <util/stream/zerocopy.h>

namespace NSkiff {
    class TUncheckedSkiffParser
    {
    public:
        explicit TUncheckedSkiffParser(IZeroCopyInput* stream);

        i64 ParseInt64();
        ui64 ParseUint64();
        double ParseDouble();
        bool ParseBoolean();

        TStringBuf ParseString32();
        TStringBuf ParseYson32();

        ui8 ParseVariant8Tag();
        ui16 ParseVariant16Tag();

        bool HasMoreData();

        void ValidateFinished();

    private:
        const void* GetData(size_t size);
        const void* GetDataViaBuffer(size_t size);

        size_t RemainingBytes() const;
        void Advance(size_t size);
        void RefillBuffer();

        template <typename T>
        T ParseSimple();

        static void ThrowInvalidBoolean(ui8 value);

    private:
        IZeroCopyInput* const Underlying_;

        TBuffer Buffer_;
        char* Position_ = nullptr;
        char* End_ = nullptr;
        bool Exhausted_ = false;
    };
}

#include "unchecked_parser-inl.h"
