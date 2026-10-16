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

    std::string Format(const std::vector<std::string>& row);

private:
    std::vector<int> MaxColumnWidth_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
