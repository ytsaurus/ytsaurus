#pragma once

#include "unchecked_parser.h"

#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>

namespace NSkiff {
    inline i64 TUncheckedSkiffParser::ParseInt64() {
        return ParseSimple<i64>();
    }

    inline ui64 TUncheckedSkiffParser::ParseUint64() {
        return ParseSimple<ui64>();
    }

    inline double TUncheckedSkiffParser::ParseDouble() {
        return ParseSimple<double>();
    }

    inline bool TUncheckedSkiffParser::ParseBoolean() {
        ui8 result = ParseSimple<ui8>();
        if (Y_UNLIKELY(result > 1)) {
            ThrowInvalidBoolean(result);
        }
        return result;
    }

    inline TStringBuf TUncheckedSkiffParser::ParseString32() {
        ui32 len = ParseSimple<ui32>();
        const void* data = GetData(len);
        return TStringBuf(static_cast<const char*>(data), len);
    }

    inline TStringBuf TUncheckedSkiffParser::ParseYson32() {
        return ParseString32();
    }

    inline ui8 TUncheckedSkiffParser::ParseVariant8Tag() {
        return ParseSimple<ui8>();
    }

    inline ui16 TUncheckedSkiffParser::ParseVariant16Tag() {
        return ParseSimple<ui16>();
    }

    template <typename T>
    inline T TUncheckedSkiffParser::ParseSimple() {
        return ReadUnaligned<T>(GetData(sizeof(T)));
    }

    inline const void* TUncheckedSkiffParser::GetData(size_t size) {
        if (Y_LIKELY(RemainingBytes() >= size)) {
            const void* result = Position_;
            Advance(size);
            return result;
        }

        return GetDataViaBuffer(size);
    }

    inline size_t TUncheckedSkiffParser::RemainingBytes() const {
        Y_ASSERT(End_ >= Position_);
        return End_ - Position_;
    }

    inline void TUncheckedSkiffParser::Advance(size_t size) {
        Y_ASSERT(size <= RemainingBytes());
        Position_ += size;
    }

    inline bool TUncheckedSkiffParser::HasMoreData() {
        if (RemainingBytes() == 0 && !Exhausted_) {
            RefillBuffer();
        }
        return !(RemainingBytes() == 0 && Exhausted_);
    }
}
