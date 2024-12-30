#pragma once

#include "public.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/library/program/program_config_mixin.h>

#include <yt/yt/library/fusion/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerProgramBase
    : public virtual TProgram
{
protected:
    TServerProgramBase();

    //! Typically invoked in the constructor of the dervied class to configure
    //! the name of the main thread.
    void SetMainThreadName(const std::string& name);
    const std::string& GetMainThreadName() const;

    virtual void ValidateOpts();
    virtual void TweakConfig();

    virtual void DoStart() = 0;

    [[noreturn]] void SleepForever();

    NFusion::IServiceLocatorPtr GetServiceLocator() const;
    NFusion::IServiceDirectoryPtr GetServiceDirectory() const;

    void Configure(const TServerProgramConfigPtr& config);

private:
    const NFusion::IServiceDirectoryPtr ServiceDirectory_;

    std::string MainThreadName_ = DefaultMainThreadName;
};

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig = void>
class TServerProgram
    : public TServerProgramBase
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TConfig, TDynamicConfig>
{
protected:
    TServerProgram();

private:
    void DoRun() final;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SERVER_PROGRAM_INL_H_
#include "server_program-inl.h"
#undef SERVER_PROGRAM_INL_H_
