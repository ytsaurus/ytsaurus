#include "tabular_formatter.h"

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

TTabularFormatter::TTabularFormatter(int columnCount)
{
    MaxColumnWidth_.resize(columnCount);
}

std::string TTabularFormatter::Format(const std::vector<std::string>& row)
{
    YT_VERIFY(ssize(row) == ssize(MaxColumnWidth_));

    std::string result;
    for (auto [str, width] : Zip(row, MaxColumnWidth_)) {
        width = std::max<int>(width, ssize(str) + 2);
        result += str;
        result += std::string(width - ssize(str), ' ');
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
