#include "helpers.h"

#include <util/digest/city.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

TLineFingerprint GetLineFingerprint(TStringBuf line)
{
    return TLineFingerprint(CityHash64(line) & LineFingerprintMask);
}

////////////////////////////////////////////////////////////////////////////////

class TFrameInput
    : public IInputStream
{
public:
    TFrameInput(IInputStream* underlying, size_t frameLength)
        : Underlying_(underlying)
        , Remaining_(frameLength)
    { }

private:
    IInputStream* const Underlying_;
    size_t Remaining_;

    size_t DoRead(void* buf, size_t len) final
    {
        auto bytesToRead = std::min(len, Remaining_);
        auto bytesRead = Underlying_->Read(buf, bytesToRead);
        Remaining_ -= bytesRead;
        return bytesRead;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IInputStream> CreateFrameInput(
    IInputStream* underlying,
    size_t frameLength)
{
    return std::make_unique<TFrameInput>(
        underlying,
        frameLength);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::pair<int, int>> ComputeMatchingRanges(
    TStringBuf line,
    const std::vector<std::string>& patterns)
{
    // Fast path.
    for (const auto& pattern : patterns) {
        if (line.find(pattern) == TStringBuf::npos) {
            return {};
        }
    }

    // Slow path.
    std::vector<std::pair<int, int>> ranges;
    for (const auto& pattern : patterns) {
        if (pattern.empty()) {
            continue;
        }
        auto remaining = line;
        int offset = 0;
        for (;;) {
            auto pos = remaining.find(pattern);
            if (pos == TStringBuf::npos) {
                break;
            }
            ranges.emplace_back(offset + pos, offset + pos + pattern.size());
            remaining.remove_prefix(pos + 1);
            offset += pos + 1;
        }
    }

    // Unite overlapping ranges.
    {
        std::sort(ranges.begin(), ranges.end());
        auto it = ranges.begin();
        while (it != ranges.end()) {
            auto next = std::next(it);
            if (next == ranges.end() || it->second < next->first) {
                ++it;
                continue;
            }
            it->second = std::max(it->second, next->second);
            ranges.erase(next);
        }
    }

    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
