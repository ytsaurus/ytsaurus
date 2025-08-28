#include "parser.h"

namespace NYa::NJson {
    bool TParserCtx::OnError(TStringBuf reason, bool end) const {
        size_t off = 0;
        TStringBuf token;

        if (ts) {
            off = ts + BufferOffset - p0;
        } else if (end) {
            off = pe - p0 + BufferOffset;
        }

        if (ts && te && te > ts) {
            token = TStringBuf(ts, te);
        }

        if (!token) {
            Handler.OnError(off, reason);
        } else {
            Handler.OnError(off, TString::Join(reason, " at token: '", token, "'"));
        }

        return false;
    }

    void TParserCtx::ShiftTsTe(const char* begin) {
        if (te) {
            te = begin + (te - ts);
        }
        ts = begin;
    }

    bool TParserCtx::Parse(IInputStream& stream) {
        size_t busy = 0;
        bool done = false;
        while (true) {
            size_t space = InputBuffer.Capacity() - busy;
            if (!space) {
                Y_ASSERT(ts == InputBuffer.Data());
                // Token is too large. Resize buffer to hold the token.
                InputBuffer.Reserve(InputBuffer.Capacity() * 2);
                if (ts) {
                    ShiftTsTe(InputBuffer.Data());
                }
                space = InputBuffer.Capacity() - busy;
            }

            char* s = InputBuffer.Data() + busy;
            size_t actuallyRead = stream.Load(s, space);
            done = actuallyRead < space;

            if (!Execute({s, actuallyRead}, done)) {
                return false;
            }

            if (done) {
                break;
            }

            BufferOffset += actuallyRead;
            if (ts) {
                busy = InputBuffer.Data() + InputBuffer.Capacity() - ts;
                if (ts != InputBuffer.Data()) {
                    MemMove(InputBuffer.Data(), ts, busy);
                    ShiftTsTe(InputBuffer.Data());
                }
            } else {
                busy = 0;
            }
        }
        return OnAfterVal() && Handler.OnEnd() || OnError("invalid or truncated", true);
    }
}
