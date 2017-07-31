#pragma once

#include "program.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProgramToolMixin
{
protected:
    TProgramToolMixin(NLastGetopt::TOpts& opts);

    bool HandleToolOptions();

private:
    TString ToolName_;
    TString ToolSpec_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
