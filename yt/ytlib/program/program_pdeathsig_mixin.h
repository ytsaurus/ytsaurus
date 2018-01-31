#pragma once

#include "program.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramPdeathsigMixin
{
protected:
    TProgramPdeathsigMixin(NLastGetopt::TOpts& opts);

    bool HandlePdeathsigOptions();

private:
    int ParentDeathSignal_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
