#pragma once

#ifndef __linux__
#error Platform must be linux to include this
#endif

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <infra/porto/api/libporto.hpp>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// NB(psushin): this class is deprecated and only used to run job proxy.
// ToDo(psushin): kill me.
class TPortoProcess
    : public TProcessBase
{
public:
    TPortoProcess(
        const TString& path,
        NContainers::IInstanceLauncherPtr containerLauncher,
        bool copyEnv = true);
    virtual void Kill(int signal) override;
    virtual NNet::IConnectionWriterPtr GetStdInWriter() override;
    virtual NNet::IConnectionReaderPtr GetStdOutReader() override;
    virtual NNet::IConnectionReaderPtr GetStdErrReader() override;

    NContainers::IInstancePtr GetInstance();

private:
    NContainers::IInstanceLauncherPtr ContainerLauncher_;
    TAtomicObject<NContainers::IInstancePtr> ContainerInstance_;
    std::vector<NPipes::TNamedPipePtr> NamedPipes_;

    virtual void DoSpawn() override;
    THashMap<TString, TString> DecomposeEnv() const;
};

DEFINE_REFCOUNTED_TYPE(TPortoProcess)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
