#include "tabular_formatter.h"

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

TTabularFormatter::TTabularFormatter(int columnCount)
{
    MaxColumnWidth_.resize(columnCount);
}

TString TTabularFormatter::Format(const std::vector<TString>& row)
{
    YT_VERIFY(ssize(row) == ssize(MaxColumnWidth_));

    TString result;
    for (auto [str, width] : Zip(row, MaxColumnWidth_)) {
        width = std::max<int>(width, ssize(str) + 2);
        result += str;
        result += TString(width - ssize(str), ' ');
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
