#pragma once

#include <library/cpp/json/common/defs.h>

#include <util/generic/buffer.h>
#include <util/generic/ymath.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYa::NJson {

    using ::NJson::TJsonCallbacks;

    struct TParserCtx {
        TJsonCallbacks& Handler;

        TBuffer Buffer;
        TBuffer InputBuffer;
        bool ValueIsString = false;
        TString String;
        bool ExpectValue = true;
        size_t BufferOffset = 0;

        const char* p0 = nullptr;
        const char* p = nullptr;
        const char* pe = nullptr;
        const char* eof = nullptr;
        const char* ts = nullptr;
        const char* te = nullptr;
        int cs = 0;
        int act = 0;

        TParserCtx(TJsonCallbacks& h, size_t initBufferSize = 1 << 16);

        bool OnError(TStringBuf reason = TStringBuf(""), bool end = false) const;

        inline bool OnVal() {
            if (Y_UNLIKELY(!ExpectValue)) {
                return false;
            }
            ExpectValue = false;
            return true;
        }

        inline bool OnNull() {
            return Y_LIKELY(OnVal())
                && Handler.OnNull();
        }

        inline bool OnTrue() {
            return Y_LIKELY(OnVal())
                && Handler.OnBoolean(true);
        }

        inline bool OnFalse() {
            return Y_LIKELY(OnVal())
                && Handler.OnBoolean(false);
        }

        inline bool OnPInt() {
            unsigned long long res = 0;
            return Y_LIKELY(OnVal())
                && TryFromString<unsigned long long>(TStringBuf(ts, te), res)
                && Handler.OnUInteger(res);
        }

        inline bool OnNInt() {
            long long res = 0;
            return Y_LIKELY(OnVal())
                && TryFromString<long long>(TStringBuf(ts, te), res)
                && Handler.OnInteger(res);
        }

        inline bool OnFlt() {
            double res = 0;
            return Y_LIKELY(OnVal())
                && TryFromString<double>(TStringBuf(ts, te), res)
                && IsFinite(res)
                && Handler.OnDouble(res);
        }

        inline bool OnMapOpen() {
            bool res = Y_LIKELY(OnVal())
                    && Handler.OnOpenMap();
            ExpectValue = true;
            return res;
        }

        inline bool OnArrOpen() {
            bool res = Y_LIKELY(OnVal())
                    && Handler.OnOpenArray();
            ExpectValue = true;
            return res;
        }

        inline bool OnString(TStringBuf s) {
            if (Y_LIKELY(OnVal())) {
                ValueIsString = true;
                String = s;
                return true;
            } else {
                return false;
            }
        }

        inline bool OnStrQ() {
            return OnString({ts + 1, te - 1});
        }

        inline bool OnStrE() {
            Buffer.Reserve(te - ts);
            char* end = UnescapeC(ts + 1, te - ts - 2, Buffer.data());
            return OnString({Buffer.data(), end});
        }

        inline bool OnMapClose() {
            ExpectValue = false;
            return Y_LIKELY(OnAfterVal())
                && Handler.OnCloseMap();
        }

        inline bool OnArrClose() {
            ExpectValue = false;
            return Y_LIKELY(OnAfterVal())
                && Handler.OnCloseArray();
        }

        inline bool OnColon() {
            if (ExpectValue) {
                return false;
            }

            ExpectValue = true;
            if (Y_LIKELY(ValueIsString)) {
                ValueIsString = false;
                return Handler.OnMapKey(String);
            } else {
                return false;
            }
        }

        inline bool OnAfterVal() {
            if (!ValueIsString) {
                return true;
            } else {
                ValueIsString = false;
                return Handler.OnString(String);
            }
        }

        inline bool OnComma() {
            if (Y_UNLIKELY(ExpectValue)) {
                return false;
            }
            ExpectValue = true;
            return OnAfterVal();
        }

        void ShiftTsTe(const char* begin);

        bool Parse(IInputStream& stream);
        bool Execute(TStringBuf data, bool done);
    };
}
