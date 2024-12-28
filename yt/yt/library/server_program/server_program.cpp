#include "server_program.h"

#include <yt/yt/library/fusion/service_directory.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TServerProgramBase::TServerProgramBase()
    : ServiceDirectory_(NFusion::CreateServiceDirectory())
{ }

void TServerProgramBase::SetMainThreadName(const std::string& name)
{
    MainThreadName_ = name;
}

const std::string& TServerProgramBase::GetMainThreadName() const
{
    return MainThreadName_;
}

void TServerProgramBase::ValidateOpts()
{ }

void TServerProgramBase::TweakConfig()
{ }

void TServerProgramBase::SleepForever()
{
    Sleep(TDuration::Max());
    YT_ABORT();
}

NFusion::IServiceLocatorPtr TServerProgramBase::GetServiceLocator() const
{
    return ServiceDirectory_;
}

NFusion::IServiceDirectoryPtr TServerProgramBase::GetServiceDirectory() const
{
    return ServiceDirectory_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
