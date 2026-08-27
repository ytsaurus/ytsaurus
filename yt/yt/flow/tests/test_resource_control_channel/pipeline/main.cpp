#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_controller.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>
#include <yt/yt/flow/library/cpp/resources/resource_controller_base.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NExample {

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Discards the random-source messages; the pipeline exists only to keep workers busy
//! while the counter resource is exercised.
class TNullFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TNullFunction);

////////////////////////////////////////////////////////////////////////////////

struct TCounterParameters
    : public virtual TYsonStruct
{
    TDuration GenerationPeriod;

    REGISTER_YSON_STRUCT(TCounterParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("generation_period", &TThis::GenerationPeriod)
            .Default(TDuration::Seconds(1));
    }
};

//! Controller side of the counter: bumps a number every generation_period and publishes it
//! as the target revision spec; reflects the per-worker applied revisions into the flow view.
class TCounterResourceController
    : public TResourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCounterParameters);

    using TResourceControllerBase::TResourceControllerBase;

    INodePtr DoBuildTargetRevisionSpec() override
    {
        auto now = TInstant::Now();
        if (Value_ == 0 || now - LastBumpTime_ >= GetParameters()->GenerationPeriod) {
            ++Value_;
            LastBumpTime_ = now;
        }
        // clang-format off
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("value").Value(Value_)
            .EndMap();
        // clang-format on
    }

    void DoCollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus) override
    {
        WorkerStatuses_ = workerStatuses;
        ControllerStatus_ = controllerStatus;
    }

    IMapNodePtr DoGetView() override
    {
        THashMap<i64, int> workersPerValue;
        for (const auto& [workerAddress, status] : WorkerStatuses_) {
            if (status && status->AppliedRevisionId) {
                ++workersPerValue[*status->AppliedRevisionId];
            }
        }
        // clang-format off
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("value").Value(Value_)
                .Item("worker_count").Value(std::ssize(WorkerStatuses_))
                // The applied id reported by each worker is the value it decoded from the
                // delivered spec payload (see TCounterResource), so this histogram proves the
                // payload -- not just the revision id -- crossed the wire.
                .Item("workers_per_value").DoMapFor(
                    workersPerValue,
                    [] (auto fluent, const auto& pair) {
                        fluent.Item(ToString(pair.first)).Value(pair.second);
                    })
                .DoIf(ControllerStatus_ && ControllerStatus_->AppliedRevisionId.has_value(), [&] (auto fluent) {
                    fluent.Item("controller_value").Value(*ControllerStatus_->AppliedRevisionId);
                })
            .EndMap()
            ->AsMap();
        // clang-format on
    }

private:
    i64 Value_ = 0;
    TInstant LastBumpTime_;
    THashMap<std::string, TWorkerResourceStatusPtr> WorkerStatuses_;
    TWorkerResourceStatusPtr ControllerStatus_;
};

//! Worker side of the counter. It reports, as its applied revision, the value it decodes from
//! the delivered spec payload -- so the reported id reflects the payload content that crossed
//! the wire, not the framework's revision stamp.
class TCounterResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCounterParameters);

    using TController = TCounterResourceController;

    using TResourceBase::TResourceBase;

    TResourceRevisionState GetRevisionState() const override
    {
        auto dynamicContext = GetDynamicContext();
        if (!dynamicContext->TargetRevision || !dynamicContext->TargetRevision->Spec) {
            return {};
        }
        auto value = dynamicContext->TargetRevision->Spec->AsMap()->GetChildValueOrThrow<i64>("value");
        return {
            .AppliedRevisionId = value,
            .TargetRevisionId = value,
        };
    }
};

YT_FLOW_DEFINE_RESOURCE(TCounterResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExample

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
