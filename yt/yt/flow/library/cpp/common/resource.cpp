#include "resource.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TFileSnapshot::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id);
    registrar.Parameter("file_providers", &TThis::FileProviders)
        .Default();
    registrar.Postprocessor([] (TThis* snapshot) {
        THROW_ERROR_EXCEPTION_UNLESS(
            snapshot->Id.Underlying() > 0,
            "File snapshot id must be positive");
        for (const auto& [name, fileProviderRevision] : snapshot->FileProviders) {
            ValidateFileProviderName(name.Underlying());
            THROW_ERROR_EXCEPTION_UNLESS(
                fileProviderRevision,
                "File snapshot provider %Qv is null",
                name);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TResourceRevision::Register(TRegistrar registrar)
{
    registrar.Parameter("revision_id", &TThis::RevisionId)
        .Default(0);
    registrar.Parameter("spec", &TThis::Spec)
        .Default();
    registrar.Parameter("active_file_snapshot", &TThis::ActiveFileSnapshot)
        .Default();
    registrar.Parameter("preparing_file_snapshot", &TThis::PreparingFileSnapshot)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TResourceLifecycleBase::TResourceLifecycleBase()
    : CancelableContext_(New<TCancelableContext>())
    , DestroyedPromise_(NewPromise<void>())
{ }

TResourceLifecycleBase::~TResourceLifecycleBase()
{
    DestroyedPromise_.Set();
}

void TResourceLifecycleBase::Stop()
{
    CancelableContext_->Cancel(TError(NYT::EErrorCode::Canceled, "Resource instance stopped"));
}

const TCancelableContextPtr& TResourceLifecycleBase::GetCancelableContext() const
{
    return CancelableContext_;
}

TFuture<void> TResourceLifecycleBase::GetDestroyedFuture() const
{
    return DestroyedPromise_.ToFuture().ToUncancelable();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
