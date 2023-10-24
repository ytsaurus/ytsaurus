#pragma once

#include <yt/yt/library/program/program.h>
#include <yt/systest/config.h>

namespace NYT::NTest {

class TProgram : public NYT::TProgram
{
public:
    TProgram();

    virtual void DoRun(const NLastGetopt::TOptsParseResult&) override;

private:
    TConfig Config_;
};

}  // namespace NYT::NTest
