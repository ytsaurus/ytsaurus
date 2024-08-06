#include "config.h"
#include "cri_api.h"

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

void TCriExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("runtime_endpoint", &TThis::RuntimeEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("image_endpoint", &TThis::ImageEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("namespace", &TThis::Namespace)
        .NonEmpty();

    registrar.Parameter("runtime_handler", &TThis::RuntimeHandler)
        .Optional();

    registrar.Parameter("base_cgroup", &TThis::BaseCgroup)
        .NonEmpty();

    registrar.Parameter("cpu_period", &TThis::CpuPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("memory_oom_group", &TThis::MemoryOOMGroup)
        .Default(false);

    registrar.Parameter("retry_error_prefixes", &TThis::RetryErrorPrefixes)
        .Default({
            // https://github.com/containerd/containerd/pull/9565
            "server is not initialized yet",
            // https://github.com/containerd/containerd/issues/9160
            "failed to create containerd task: failed to create shim task: OCI runtime create failed: runc create failed: unable to create new parent process: namespace path: lstat /proc/0/ns/ipc: no such file or directory: unknown",
        });
}

////////////////////////////////////////////////////////////////////////////////

void TCriAuthConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("username", &TThis::Username)
        .Optional();

    registrar.Parameter("password", &TThis::Password)
        .Optional();

    registrar.Parameter("auth", &TThis::Auth)
        .Optional();

    registrar.Parameter("server_address", &TThis::ServerAddress)
        .Optional();

    registrar.Parameter("identity_token", &TThis::IdentityToken)
        .Optional();

    registrar.Parameter("registry_token", &TThis::RegistryToken)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
