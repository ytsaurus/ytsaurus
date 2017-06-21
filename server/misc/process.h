#pragma once

#include <yt/server/containers/public.h>

#include <yt/core/misc/process.h>

#include <yt/contrib/portoapi/libporto.hpp>

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
    virtual NPipes::TAsyncWriterPtr GetStdInWriter() override;
    virtual NPipes::TAsyncReaderPtr GetStdOutReader() override;
    virtual NPipes::TAsyncReaderPtr GetStdErrReader() override;

private:
    NContainers::IInstancePtr ContainerInstance_;
    std::vector<NPipes::TNamedPipePtr> NamedPipes_;
    virtual void DoSpawn() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
