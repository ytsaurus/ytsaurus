#pragma once

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <library/cpp/porto/libporto.hpp>

namespace NYT::NContainers {

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
    void Kill(int signal) override;
    NNet::IConnectionWriterPtr GetStdInWriter() override;
    NNet::IConnectionReaderPtr GetStdOutReader() override;
    NNet::IConnectionReaderPtr GetStdErrReader() override;

    NContainers::IInstancePtr GetInstance();

private:
    NContainers::IInstanceLauncherPtr ContainerLauncher_;
    TAtomicObject<NContainers::IInstancePtr> ContainerInstance_;
    std::vector<NPipes::TNamedPipePtr> NamedPipes_;

    void DoSpawn() override;
    THashMap<TString, TString> DecomposeEnv() const;
};

DEFINE_REFCOUNTED_TYPE(TPortoProcess)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
