#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <util/system/getpid.h>

namespace NYT::NFlow::NCompanionTest {

////////////////////////////////////////////////////////////////////////////////

struct TGreetingParameters
    : public NYTree::TYsonStruct
{
    std::string Greeting;

    REGISTER_YSON_STRUCT(TGreetingParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("greeting", &TThis::Greeting)
            .Default();
    }
};

struct TGreetingDynamicParameters
    : public NYTree::TYsonStruct
{
    std::string Suffix;

    REGISTER_YSON_STRUCT(TGreetingDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("suffix", &TThis::Suffix)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TGreetingDependencyDynamicParameters
    : public NYTree::TYsonStruct
{
    std::string Value;

    REGISTER_YSON_STRUCT(TGreetingDependencyDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGreetingDependencyResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TGreetingDependencyDynamicParameters);

    using TResourceBase::TResourceBase;

    std::string GetValue() const
    {
        return GetDynamicParameters()->Value;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Companion-hosted resource under test: Load records the pid of the serving
//! companion process; the static "greeting" and dynamic "suffix" parameters
//! are read through the TResourceBase accessors, so a reconfigure delivered by
//! the worker is observable as a fresh suffix value.
class TGreetingResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TGreetingParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TGreetingDynamicParameters);

    using TResourceBase::TResourceBase;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override
    {
        DependencyValue_ = GetOrCrash(dependencies, TResourceId("dependency"))
            ->As<TGreetingDependencyResource>()
            ->GetValue();
        InitPid_ = static_cast<i64>(GetPID());
        YT_TLOG_INFO("Greeting resource loaded")
            .With("Greeting", GetGreeting())
            .With("Suffix", GetSuffix())
            .With("DependencyValue", DependencyValue_)
            .With("Pid", InitPid_);
        return OKFuture;
    }

    std::string GetGreeting() const
    {
        return GetParameters()->Greeting;
    }

    //! Reads the freshest dynamic parameters: TResourceBase reparses them on
    //! every Reconfigure.
    std::string GetSuffix() const
    {
        return GetDynamicParameters()->Suffix;
    }

    i64 GetInitPid() const
    {
        return InitPid_;
    }

    const std::string& GetDependencyValue() const
    {
        return DependencyValue_;
    }

private:
    i64 InitPid_ = -1;
    std::string DependencyValue_;
};

////////////////////////////////////////////////////////////////////////////////

//! Copies the current state of the greeting resource into every output row, so
//! the test can assert on the resource's init parameters, applied dynamic
//! parameters and the serving companion pid.
class TGreetingMapperFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        Resource_ = initContext->GetStaticResource("greeting_view")->As<TGreetingResource>();
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto builder = context->MakeOutputMessageBuilder("mapped");
        builder.Payload().Set<std::string>(GetColumnValue<std::string>(message, "key"), "key");
        builder.Payload().Set<std::string>(Resource_->GetGreeting(), "greeting");
        builder.Payload().Set<std::string>(Resource_->GetSuffix(), "suffix");
        builder.Payload().Set<std::string>(Resource_->GetDependencyValue(), "dependency_value");
        builder.Payload().Set<i64>(Resource_->GetInitPid(), "pid");
        output->AddMessage(builder.Finish());
    }

private:
    TIntrusivePtr<TGreetingResource> Resource_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionTest

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddTransform<NYT::NFlow::NCompanionTest::TGreetingMapperFunction>("mapper");
    pipeline.AddResource<NYT::NFlow::NCompanionTest::TGreetingDependencyResource>();
    pipeline.AddResource<NYT::NFlow::NCompanionTest::TGreetingResource>();
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
