#pragma once

#include "program.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramCgroupMixin
{
protected:
    TProgramCgroupMixin(NLastGetopt::TOpts& opts);

    bool HandleCgroupOptions();

private:
    std::vector<TString> CgroupPaths_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
