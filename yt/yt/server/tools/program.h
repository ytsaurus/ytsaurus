#pragma once

#include <yt/yt/library/program/program.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

class TToolsProgram
    : public TProgram
{
public:
    TToolsProgram();

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override;

private:
    TString ToolName_;
    TString ToolSpec_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
