#include "cri_executor.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NContainers::NCri {

using namespace NRpc;
using namespace NRpc::NGrpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("CRI");

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TCriDescriptor& desc, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v (%s)", desc.Id.substr(0, 12), desc.Name);
}

void FormatValue(TStringBuilderBase* builder, const TCriPodDescriptor& desc, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v (%s)", desc.Id.substr(0, 12), desc.Name);
}

void FormatValue(TStringBuilderBase* builder, const TCriImageDescriptor& desc, TStringBuf /*spec*/)
{
    builder->AppendString(desc.Image);
}

static TError DecodeExitCode(int exitCode, const TString& reason)
{
    if (exitCode == 0) {
        return TError();
    }

    // FIXME(khkebnikov) map reason == "OOMKilled"

    // Common bash notation for signals: 128 + signal
    if (exitCode > 128) {
        int signalNumber = exitCode - 128;
        return TError(
            EProcessErrorCode::Signal,
            "Process terminated by signal %v",
            signalNumber)
            << TErrorAttribute("signal", signalNumber)
            << TErrorAttribute("reason", reason);
    }

    // FIXME(khkebnikov) check these
    // 125 - container failed to run
    // 126 - non executable
    // 127 - command not found
    // 128 - invalid exit code
    // 255 - exit code out of range

    return TError(
        EProcessErrorCode::NonZeroExitCode,
        "Process exited with code %v",
        exitCode)
        << TErrorAttribute("exit_code", exitCode)
        << TErrorAttribute("reason", reason);
}

////////////////////////////////////////////////////////////////////////////////

class TCriProcess
    : public TProcessBase
{
public:
    TCriProcess(
        const TString& path,
        ICriExecutorPtr executor,
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& pod,
        TCriPodSpecPtr podSpec,
        TDuration pollPeriod = TDuration::MilliSeconds(100))
        : TProcessBase(path)
        , Executor_(std::move(executor))
        , ContainerSpec_(std::move(containerSpec))
        , PodDesc_(pod)
        , PodSpec_(std::move(podSpec))
        , PollPeriod_(pollPeriod)
    {
        // Just for symmetry with sibling classes.
        AddArgument(Path_);
    }

    void Kill(int /*signal*/) override
    {
        WaitFor(Executor_->StopContainer(ContainerDesc_))
            .ThrowOnError();
    }

    NNet::IConnectionWriterPtr GetStdInWriter() override
    {
        THROW_ERROR_EXCEPTION("Not implemented for CRI process");
    }

    NNet::IConnectionReaderPtr GetStdOutReader() override
    {
        THROW_ERROR_EXCEPTION("Not implemented for CRI process");
    }

    NNet::IConnectionReaderPtr GetStdErrReader() override
    {
        THROW_ERROR_EXCEPTION("Not implemented for CRI process");
    }

private:
    const ICriExecutorPtr Executor_;
    const TCriContainerSpecPtr ContainerSpec_;
    const TCriPodDescriptor PodDesc_;
    const TCriPodSpecPtr PodSpec_;
    const TDuration PollPeriod_;

    TCriDescriptor ContainerDesc_;

    TPeriodicExecutorPtr AsyncWaitExecutor_;

    void DoSpawn() override
    {
        if (ContainerSpec_->Command.empty()) {
            ContainerSpec_->Command = {Path_};
        }
        ContainerSpec_->Arguments = std::vector<TString>(Args_.begin() + 1, Args_.end());
        ContainerSpec_->WorkingDirectory = WorkingDirectory_;

        ContainerSpec_->BindMounts.emplace_back(
            NCri::TCriBindMount {
                .ContainerPath = WorkingDirectory_,
                .HostPath = WorkingDirectory_,
                .ReadOnly = false,
            }
        );

        for (const auto& keyVal : Env_) {
            TStringBuf key, val;
            if (TStringBuf(keyVal).TrySplit('=', key, val)) {
                ContainerSpec_->Environment[key] = val;
            }
        }

        ContainerDesc_ = WaitFor(Executor_->CreateContainer(ContainerSpec_, PodDesc_, PodSpec_))
            .ValueOrThrow();

        YT_LOG_DEBUG("Spawning process (Command: %v, Container: %v)", ContainerSpec_->Command[0], ContainerDesc_);
        WaitFor(Executor_->StartContainer(ContainerDesc_))
            .ThrowOnError();

        // FIXME(khkebnikov) replace polling with CRI event
        AsyncWaitExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TCriProcess::PollContainerStatus, MakeStrong(this)),
            PollPeriod_);

        AsyncWaitExecutor_->Start();
    }

    void PollContainerStatus()
    {
        Executor_->GetContainerStatus(ContainerDesc_)
            .SubscribeUnique(BIND(&TCriProcess::OnContainerStatus, MakeStrong(this)));
    }

    void OnContainerStatus(TErrorOr<TCriRuntimeApi::TRspContainerStatusPtr>&& responseOrError)
    {
        auto response = responseOrError.ValueOrThrow();
        if (!response->has_status()) {
            return;
        }
        auto status = response->status();
        if (status.state() == NProto::CONTAINER_EXITED) {
            auto error = DecodeExitCode(status.exit_code(), status.reason());
            YT_LOG_DEBUG("Process finished (Container: %v, Error: %v)", ContainerDesc_, error);
            YT_UNUSED_FUTURE(AsyncWaitExecutor_->Stop());
            FinishedPromise_.TrySet(error);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCriProcess)

////////////////////////////////////////////////////////////////////////////////

class TCriExecutor
    : public ICriExecutor
{
public:
    TCriExecutor(
        TCriExecutorConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config_(std::move(config))
        , RuntimeApi_(channelFactory->CreateChannel(Config_->RuntimeEndpoint))
        , ImageApi_(channelFactory->CreateChannel(Config_->ImageEndpoint))
    { }

    TString GetPodCgroup(TString podName) const override
    {
        TStringBuilder cgroup;
        cgroup.AppendString(Config_->BaseCgroup);
        cgroup.AppendString("/");
        cgroup.AppendString(podName);
        if (Config_->BaseCgroup.EndsWith(SystemdSliceSuffix)) {
            cgroup.AppendString(SystemdSliceSuffix);
        }
        return cgroup.Flush();
    }

    TFuture<TCriRuntimeApi::TRspStatusPtr> GetRuntimeStatus(bool verbose = false) override
    {
        auto req = RuntimeApi_.Status();
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriRuntimeApi::TRspListPodSandboxPtr> ListPodSandbox(
        std::function<void(NProto::PodSandboxFilter&)> initFilter = nullptr) override
    {
        auto req = RuntimeApi_.ListPodSandbox();

        {
            auto* filter = req->mutable_filter();

            if (auto namespace_ = Config_->Namespace) {
                auto& labels = *filter->mutable_label_selector();
                labels[YTPodNamespaceLabel] = namespace_;
            }

            if (initFilter) {
                initFilter(*filter);
            }
        }

        return req->Invoke();
    }

    TFuture<TCriRuntimeApi::TRspListContainersPtr> ListContainers(
        std::function<void(NProto::ContainerFilter&)> initFilter = nullptr) override
    {
        auto req = RuntimeApi_.ListContainers();

        {
            auto* filter = req->mutable_filter();

            if (auto namespace_ = Config_->Namespace) {
                auto& labels = *filter->mutable_label_selector();
                labels[YTPodNamespaceLabel] = namespace_;
            }

            if (initFilter) {
                initFilter(*filter);
            }
        }

        return req->Invoke();
    }

    TFuture<void> ForEachPodSandbox(
        const TCallback<void(const TCriPodDescriptor&, const NProto::PodSandbox&)>& callback,
        std::function<void(NProto::PodSandboxFilter&)> initFilter) override
    {
        return ListPodSandbox(initFilter).Apply(BIND([=] (const TCriRuntimeApi::TRspListPodSandboxPtr& rsp) {
            for (const auto& pod : rsp->items()) {
                TCriPodDescriptor desc{.Name=pod.metadata().name(), .Id=pod.id()};
                callback(desc, pod);
            }
        }));
    }

    TFuture<void> ForEachContainer(
        const TCallback<void(const TCriDescriptor&, const NProto::Container&)>& callback,
        std::function<void(NProto::ContainerFilter&)> initFilter = nullptr) override
    {
        return ListContainers(initFilter).Apply(BIND([=] (const TCriRuntimeApi::TRspListContainersPtr& rsp) {
            for (const auto& ct : rsp->containers()) {
                TCriDescriptor desc{.Name=ct.metadata().name(), .Id=ct.id()};
                callback(desc, ct);
            }
        }));
    }

    TFuture<TCriRuntimeApi::TRspPodSandboxStatusPtr> GetPodSandboxStatus(
        const TCriPodDescriptor& pod, bool verbose = false) override
    {
        auto req = RuntimeApi_.PodSandboxStatus();
        req->set_pod_sandbox_id(pod.Id);
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriRuntimeApi::TRspContainerStatusPtr> GetContainerStatus(
        const TCriDescriptor& ct, bool verbose = false) override
    {
        auto req = RuntimeApi_.ContainerStatus();
        req->set_container_id(ct.Id);
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriPodDescriptor> RunPodSandbox(TCriPodSpecPtr podSpec) override
    {
        auto req = RuntimeApi_.RunPodSandbox();

        FillPodSandboxConfig(req->mutable_config(), *podSpec);

        if (Config_->RuntimeHandler) {
            req->set_runtime_handler(Config_->RuntimeHandler);
        }

        return req->Invoke().Apply(BIND([name = podSpec->Name] (const TCriRuntimeApi::TRspRunPodSandboxPtr& rsp) -> TCriPodDescriptor {
            return TCriPodDescriptor{.Name = name, .Id = rsp->pod_sandbox_id()};
        }));
    }

    TFuture<void> StopPodSandbox(const TCriPodDescriptor& pod) override
    {
        auto req = RuntimeApi_.StopPodSandbox();
        req->set_pod_sandbox_id(pod.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> RemovePodSandbox(const TCriPodDescriptor& pod) override
    {
        auto req = RuntimeApi_.RemovePodSandbox();
        req->set_pod_sandbox_id(pod.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> UpdatePodResources(
        const TCriPodDescriptor& /*pod*/,
        const TCriContainerResources& /*resources*/) override
    {
        return MakeFuture(TError("Not implemented"));
    }

    TFuture<TCriDescriptor> CreateContainer(
        TCriContainerSpecPtr ctSpec,
        const TCriPodDescriptor& pod,
        TCriPodSpecPtr podSpec) override
    {
        auto req = RuntimeApi_.CreateContainer();
        req->set_pod_sandbox_id(pod.Id);

        auto* config = req->mutable_config();

        {
            auto* metadata = config->mutable_metadata();
            metadata->set_name(ctSpec->Name);
        }

        {
            auto& labels = *config->mutable_labels();

            for (const auto& [key, val] : ctSpec->Labels) {
                labels[key] = val;
            }

            labels[YTPodNamespaceLabel] = Config_->Namespace;
            labels[YTPodNameLabel] = podSpec->Name;
            labels[YTContainerNameLabel] = ctSpec->Name;
        }

        FillImageSpec(config->mutable_image(), ctSpec->Image);

        for (const auto& mountSpec : ctSpec->BindMounts) {
            auto* mount = config->add_mounts();
            mount->set_container_path(mountSpec.ContainerPath);
            mount->set_host_path(mountSpec.HostPath);
            mount->set_readonly(mountSpec.ReadOnly);
            mount->set_propagation(NProto::PROPAGATION_PRIVATE);
        }

        {
            ToProto(config->mutable_command(), ctSpec->Command);
            ToProto(config->mutable_args(), ctSpec->Arguments);

            config->set_working_dir(ctSpec->WorkingDirectory);

            for (const auto& [key, val] : ctSpec->Environment) {
                auto* env = config->add_envs();
                env->set_key(key);
                env->set_value(val);
            }
        }

        {
            auto* linux = config->mutable_linux();
            FillLinuxContainerResources(linux->mutable_resources(), ctSpec->Resources);

            auto* security = linux->mutable_security_context();

            auto* namespaces = security->mutable_namespace_options();
            namespaces->set_network(NProto::NODE);

            security->set_readonly_rootfs(ctSpec->ReadOnlyRootFS);

            if (ctSpec->Credentials.Uid) {
                security->mutable_run_as_user()->set_value(*ctSpec->Credentials.Uid);
            }
            if (ctSpec->Credentials.Gid) {
                security->mutable_run_as_group()->set_value(*ctSpec->Credentials.Gid);
            }
            ToProto(security->mutable_supplemental_groups(), ctSpec->Credentials.Groups);
        }

        FillPodSandboxConfig(req->mutable_sandbox_config(), *podSpec);

        return req->Invoke().Apply(BIND([name = ctSpec->Name] (const TCriRuntimeApi::TRspCreateContainerPtr& rsp) -> TCriDescriptor {
            return TCriDescriptor{.Name = "", .Id = rsp->container_id()};
        }));
    }

    TFuture<void> StartContainer(const TCriDescriptor& ct) override
    {
        auto req = RuntimeApi_.StartContainer();
        req->set_container_id(ct.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> StopContainer(const TCriDescriptor& ct, TDuration timeout = TDuration::Zero()) override
    {
        auto req = RuntimeApi_.StopContainer();
        req->set_container_id(ct.Id);
        req->set_timeout(timeout.Seconds());
        return req->Invoke().AsVoid();
    }

    TFuture<void> RemoveContainer(const TCriDescriptor& ct) override
    {
        auto req = RuntimeApi_.RemoveContainer();
        req->set_container_id(ct.Id);
        return req->Invoke().AsVoid();
    }

    TFuture<void> UpdateContainerResources(const TCriDescriptor& ct, const TCriContainerResources& resources) override
    {
        auto req = RuntimeApi_.UpdateContainerResources();
        req->set_container_id(ct.Id);
        FillLinuxContainerResources(req->mutable_linux(), resources);
        return req->Invoke().AsVoid();
    }

    void CleanNamespace() override
    {
        YT_VERIFY(Config_->Namespace);
        auto pods = WaitFor(ListPodSandbox())
            .ValueOrThrow();

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(pods->items_size());
            for (const auto& pod : pods->items()) {
                TCriPodDescriptor podDesc{.Name = pod.metadata().name(), .Id = pod.id() };
                futures.push_back(StopPodSandbox(podDesc));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(pods->items_size());
            for (const auto& pod : pods->items()) {
                TCriPodDescriptor podDesc{.Name = pod.metadata().name(), .Id = pod.id()};
                futures.push_back(RemovePodSandbox(podDesc));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }
    }

    void CleanPodSandbox(const TCriPodDescriptor& pod) override
    {
        auto containers = WaitFor(ListContainers([=] (NProto::ContainerFilter& filter) {
                filter.set_pod_sandbox_id(pod.Id);
            }))
            .ValueOrThrow();

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(containers->containers_size());
            for (const auto& ct : containers->containers()) {
                TCriDescriptor ctDesc{.Name = ct.metadata().name(), .Id = ct.id()};
                futures.push_back(StopContainer(ctDesc));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }

        {
            std::vector<TFuture<void>> futures;
            futures.reserve(containers->containers_size());
            for (const auto& ct : containers->containers()) {
                TCriDescriptor ctDesc{.Name = ct.metadata().name(), .Id = ct.id()};
                futures.push_back(RemoveContainer(ctDesc));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }
    }

    TFuture<TCriImageApi::TRspListImagesPtr> ListImages(
        std::function<void(NProto::ImageFilter&)> initFilter = nullptr) override
    {
        auto req = ImageApi_.ListImages();
        if (initFilter) {
            initFilter(*req->mutable_filter());
        }
        return req->Invoke();
    }

    TFuture<TCriImageApi::TRspImageStatusPtr> GetImageStatus(
        const TCriImageDescriptor& image,
        bool verbose = false) override
    {
        auto req = ImageApi_.ImageStatus();
        FillImageSpec(req->mutable_image(), image);
        req->set_verbose(verbose);
        return req->Invoke();
    }

    TFuture<TCriImageDescriptor> PullImage(
        const TCriImageDescriptor& image,
        TCriPodSpecPtr podSpec = nullptr) override
    {
        auto req = ImageApi_.PullImage();
        FillImageSpec(req->mutable_image(), image);
        if (podSpec) {
            FillPodSandboxConfig(req->mutable_sandbox_config(), *podSpec);
        }
        return req->Invoke().Apply(BIND([] (const TCriImageApi::TRspPullImagePtr& rsp) -> TCriImageDescriptor {
            return TCriImageDescriptor{.Image = rsp->image_ref()};
        }));
    }

    TFuture<void> RemoveImage(const TCriImageDescriptor& image) override
    {
        auto req = ImageApi_.RemoveImage();
        FillImageSpec(req->mutable_image(), image);
        return req->Invoke().AsVoid();
    }

    TProcessBasePtr CreateProcess(
        const TString& path,
        TCriContainerSpecPtr containerSpec,
        const TCriPodDescriptor& pod,
        TCriPodSpecPtr podSpec) override
    {
        return New<TCriProcess>(path, this, std::move(containerSpec), pod, std::move(podSpec));
    }

private:
    const TCriExecutorConfigPtr Config_;
    TCriRuntimeApi RuntimeApi_;
    TCriImageApi ImageApi_;

    void FillLinuxContainerResources(NProto::LinuxContainerResources* resources, const TCriContainerResources& spec)
    {
        auto* unified = resources->mutable_unified();

        if (spec.CpuLimit) {
            i64 period = Config_->CpuPeriod.MicroSeconds();
            i64 quota = period * *spec.CpuLimit;

            resources->set_cpu_period(period);
            resources->set_cpu_quota(quota);
        }

        if (spec.MemoryLimit) {
            resources->set_memory_limit_in_bytes(*spec.MemoryLimit);
        }

        if (spec.MemoryRequest) {
            (*unified)["memory.low"] = ToString(*spec.MemoryRequest);
        }
    }

    void FillPodSandboxConfig(NProto::PodSandboxConfig* config, const TCriPodSpec& spec)
    {
        {
            auto* metadata = config->mutable_metadata();
            metadata->set_namespace_(Config_->Namespace);
            metadata->set_name(spec.Name);
            metadata->set_uid(spec.Name);
        }

        {
            auto& labels = *config->mutable_labels();
            labels[YTPodNamespaceLabel] = Config_->Namespace;
            labels[YTPodNameLabel] = spec.Name;
        }

        {
            auto* linux = config->mutable_linux();
            linux->set_cgroup_parent(GetPodCgroup(spec.Name));

            auto* security = linux->mutable_security_context();
            auto* namespaces = security->mutable_namespace_options();
            namespaces->set_network(NProto::NODE);
        }
    }

    void FillImageSpec(NProto::ImageSpec* spec, const TCriImageDescriptor& image)
    {
        spec->set_image(image.Image);
    }

};

////////////////////////////////////////////////////////////////////////////////

ICriExecutorPtr CreateCriExecutor(TCriExecutorConfigPtr config)
{
    return New<TCriExecutor>(
        std::move(config),
        GetGrpcChannelFactory());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
