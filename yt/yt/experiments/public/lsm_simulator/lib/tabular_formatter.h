#pragma once

#include <util/generic/fwd.h>
#include <util/generic/string.h>

#include <algorithm>
#include <vector>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TTabularFormatter
{
public:
    explicit TTabularFormatter(int columnCount);

    TString Format(const std::vector<TString>& row);

private:
    std::vector<int> MaxColumnWidth_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
