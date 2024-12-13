#pragma once

#include "public.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerProgram
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
{
protected:
    TServerProgram();

    //! Typically invoked in the constructor of the dervied class to configure
    //! the name of the main thread.
    void SetMainThreadName(const std::string& name);

    virtual void DoStart() = 0;

private:
    std::string MainThreadName_ = DefaultMainThreadName;

    void DoRun() final;
    void Configure();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
