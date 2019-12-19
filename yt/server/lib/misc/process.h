#pragma once

#ifndef __linux__
#error Platform must be linux to include this
#endif

#include <yt/server/lib/containers/public.h>

#include <yt/library/process/process.h>

#include <infra/porto/api/libporto.hpp>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPortoProcess
    : public TProcessBase
{
public:
    TPortoProcess(
        const TString& path,
        NContainers::IInstancePtr containerInstance,
        bool copyEnv = true,
        TDuration pollPeriod = TDuration::MilliSeconds(100));
    virtual void Kill(int signal) override;
    virtual NNet::IConnectionWriterPtr GetStdInWriter() override;
    virtual NNet::IConnectionReaderPtr GetStdOutReader() override;
    virtual NNet::IConnectionReaderPtr GetStdErrReader() override;

private:
    NContainers::IInstancePtr ContainerInstance_;
    std::vector<NPipes::TNamedPipePtr> NamedPipes_;
    virtual void DoSpawn() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
