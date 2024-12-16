#pragma once

#include "public.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/library/program/program_config_mixin.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig = void>
class TServerProgram
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TConfig, TDynamicConfig>
{
protected:
    explicit TServerProgram(bool requireConfigArgument = true);

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

#define SERVER_PROGRAM_INL_H_
#include "server_program-inl.h"
#undef SERVER_PROGRAM_INL_H_
