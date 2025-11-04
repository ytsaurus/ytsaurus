#include "line_reader.h"

#include "private.h"

#include <yt/yt/core/misc/checksum.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

TLineReader::TLineReader(IInputStream* input)
    : Input_(input)
    , Buffer_(BufferCapacity)
{ }

std::optional<TStringBuf> TLineReader::ReadLine()
{
    ScratchLine_.clear();
    for (;;) {
        if (BufferPosition_ == BufferSize_) {
            BufferPosition_ = 0;
            BufferSize_ = Input_->Read(Buffer_.data(), BufferCapacity);
            Checksum_ = NYT::GetChecksum(TRef(Buffer_.data(), BufferSize_), Checksum_);
            if (BufferSize_ == 0) {
                if (ScratchLine_.empty()) {
                    return std::nullopt;
                }
                return ScratchLine_;
            }
        }

        auto newlinePosition = TStringBuf(Buffer_.data() + BufferPosition_, Buffer_.data() + BufferSize_).find('\n');
        if (newlinePosition != TStringBuf::npos) {
            ScratchLine_.append(Buffer_.begin() + BufferPosition_, Buffer_.begin() + BufferPosition_ + newlinePosition);
            BufferPosition_ += newlinePosition + 1;
            return ScratchLine_;
        }

        ScratchLine_.append(Buffer_.begin() + BufferPosition_, BufferSize_ - BufferPosition_);
        BufferPosition_ = BufferSize_;
    }
}

TChecksum TLineReader::GetChecksum() const
{
    return Checksum_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
