#include "spec_manager.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSpecManager::TSpecManager(
    ISpecManagerHost* host,
    NScheduler::TOperationSpecBasePtr spec,
    NLogging::TLogger logger)
    : Host_(host)
    , TestingSpec_(spec->TestingOperationOptions->PatchSpecProtocol)
    , OriginalSpec_(std::move(spec))
    , DynamicSpec_(OriginalSpec_)
    , Logger(std::move(logger))
{ }

void TSpecManager::InitializeReviving(INodePtr&& cumulativeSpecPatch)
{
    InitialCumulativeSpecPatch_ = std::move(cumulativeSpecPatch);
    UpdateConfigurator_.emplace(Host_->ConfigureUpdate());


    // NB(coteeq): If there is no snapshot, then it's okay to initialize cleanly with the new spec.
    // If there is something wrong with the new spec, then we will find it out in the new lifecycle.
    // But if we were to update spec during revive with clean start, it would complicate things, as
    // code inside update cannot rely on any internal state.
    if (InitialCumulativeSpecPatch_) {
        auto currentSpec = GetSpec();
        auto newSpec = PatchNode(ConvertToNode(currentSpec), InitialCumulativeSpecPatch_);
        DynamicSpec_.Exchange(Host_->ParseTypedSpec(newSpec));
    }
}

void TSpecManager::InitializeClean()
{
    UpdateConfigurator_.emplace(Host_->ConfigureUpdate());
}

void TSpecManager::ValidateSpecPatch(const INodePtr& /*newCumulativeSpecPatch*/) const
{
    if (TestingSpec_ && TestingSpec_->FailValidate) {
        THROW_ERROR_EXCEPTION("Failed to validate patch because of spec option");
    }
}

void TSpecManager::ApplySpecPatch(INodePtr newCumulativeSpecPatch)
{
    DoApply(
        GetSpec(),
        std::move(newCumulativeSpecPatch),
        /*shouldFail*/ TestingSpec_ && TestingSpec_->FailApply);
}

void TSpecManager::ApplySpecPatchReviving()
{
    if (InitialCumulativeSpecPatch_) {
        // NB(coteeq): We are updating against current spec, because we have no
        // idea what is in the snapshot.
        // Maybe, there is info about all patches. Maybe there is no info about
        // any patches at all. Either way, we can update against original spec
        // and all handlers will make sure that the current state is concistent
        // with all patches.
        DoApply(
            OriginalSpec_,
            InitialCumulativeSpecPatch_,
            /*shouldFail*/ TestingSpec_ && TestingSpec_->FailRevive);
    } else {
        YT_LOG_INFO("No initial spec patch");
    }
}

void TSpecManager::DoApply(
    const TOperationSpecBasePtr& currentSpec,
    INodePtr patch,
    bool shouldFail)
{
    YT_LOG_INFO(
        "Applying spec patch (OldPatch: %v, NewPatch: %v)",
        InitialCumulativeSpecPatch_
            ? ConvertToYsonString(InitialCumulativeSpecPatch_, EYsonFormat::Text).ToString()
            : TString("<null>"),
        ConvertToYsonString(patch, EYsonFormat::Text));

    auto newSpec = PatchNode(ConvertToNode(currentSpec), patch);

    YT_VERIFY(UpdateConfigurator_);

    try {
        auto safeAssertGuard = Host_->CreateSafeAssertionGuard();
        TForbidContextSwitchGuard contextSwitchGuard;
        if (shouldFail) {
            THROW_ERROR_EXCEPTION("Failed to apply patch because of testing spec option");
        }

        auto parsedNewSpec = Host_->ParseTypedSpec(newSpec);

        // NB: This works, because everything here is (hopefully) of the same
        // static type inside. Configurator's and both specs' type should be
        // T{OperationType}Spec.
        //
        // |Update| will check that the metas of all three things are the same.
        NYsonStructUpdate::Update(*UpdateConfigurator_, currentSpec, parsedNewSpec);
        DynamicSpec_.Exchange(std::move(parsedNewSpec));
        // Just for informational purposes.
        InitialCumulativeSpecPatch_ = std::move(patch);
    } catch (const std::exception& ex) {
        Host_->ProcessSafeException(ex);
        YT_LOG_ERROR(
            ex,
            "Failed to apply patch (Patch: %v)",
            ConvertToYsonString(patch, EYsonFormat::Text).ToString());
        THROW_ERROR_EXCEPTION(
            EErrorCode::ExceptionLeadingToOperationFailure,
            "Failed to apply spec patch")
            << ex;
    } catch (const TAssertionFailedException& ex) {
        Host_->ProcessSafeException(ex);
        YT_LOG_ERROR(
            "Failed to apply patch (Patch: %v)",
            ConvertToYsonString(patch, EYsonFormat::Text).ToString());
        THROW_ERROR_EXCEPTION(
            EErrorCode::ExceptionLeadingToOperationFailure,
            "Operation controller crashed while applying spec patch");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
