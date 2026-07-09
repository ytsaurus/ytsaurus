#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

class TEnricher
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(BankJoiner_, "/bank");
        initContext->InitExternalStateClient(UserMetadataJoiner_, "/user_metadata");
    }

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        const auto& joiners = GetSpec()->ExternalStateJoiners;
        if (!joiners.at("/bank")->AutoPreload) {
            WaitFor(BankJoiner_.PreloadKeyStates(input)).ThrowOnError();
        }
        if (!joiners.at("/user_metadata")->AutoPreload) {
            WaitFor(UserMetadataJoiner_.PreloadKeyStates(input)).ThrowOnError();
        }

        for (const auto& message : input->GetMessages()) {
            auto bankState = BankJoiner_.GetState(message);
            auto userState = UserMetadataJoiner_.GetState(message);
            auto userId = GetColumnValue<TString>(message, "UserId");

            auto builder = MakeOutputMessageBuilder();
            builder.Payload().Set<TString>(userId, "UserId");
            builder.Payload().Set<TString>(bankState->GetColumnValue<TString>("BankName"), "BankName");
            builder.Payload().Set<TString>(userState->GetColumnValue<TString>("UserNickname"), "UserNickname");
            output->AddMessage(builder.Finish());
        }
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> BankJoiner_;
    TJoinedStateKeyClient<TSimpleExternalState> UserMetadataJoiner_;
};

YT_FLOW_DEFINE_COMPUTATION(TEnricher);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    Initialize(argc, argv);
    return TSimpleRunnerProgram().Run(argc, argv);
}
