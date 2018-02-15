#include "unchecked_parser.h"

#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>

namespace NSkiff {
    TUncheckedSkiffParser::TUncheckedSkiffParser(IZeroCopyInput* underlying)
        : Underlying_(underlying)
        , Buffer_(512 * 1024)
    {
    }

    const void* TUncheckedSkiffParser::GetDataViaBuffer(size_t size) {
        Buffer_.Clear();
        Buffer_.Reserve(size);
        while (Buffer_.Size() < size) {
            size_t toCopy = Min(size - Buffer_.Size(), RemainingBytes());
            Buffer_.Append(Position_, toCopy);
            Advance(toCopy);

            if (RemainingBytes() == 0) {
                RefillBuffer();
                if (Y_UNLIKELY(Exhausted_ && Buffer_.Size() < size)) {
                    ythrow yexception() << "Premature end of stream while parsing Skiff";
                }
            }
        }
        return Buffer_.Data();
    }

    void TUncheckedSkiffParser::RefillBuffer() {
        // request next buffer from underlying stream
        size_t bufferSize = Underlying_->Next(&Position_);
        End_ = Position_ + bufferSize;
        if (bufferSize == 0) {
            // EOF in underlying stream
            Exhausted_ = true;
        }
    }

    void TUncheckedSkiffParser::ThrowInvalidBoolean(ui8 value) {
        ythrow yexception() << "Invalid boolean value " << value;
    }

    void TUncheckedSkiffParser::ValidateFinished() {
    }
}
