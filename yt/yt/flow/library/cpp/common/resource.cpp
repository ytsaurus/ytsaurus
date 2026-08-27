#include "resource.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TFileSnapshot::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id);
    registrar.Parameter("file_sources", &TThis::FileSources)
        .Default();
    registrar.Postprocessor([] (TThis* snapshot) {
        THROW_ERROR_EXCEPTION_UNLESS(
            snapshot->Id.Underlying() > 0,
            "File snapshot id must be positive");
        for (const auto& [name, fileSourceRevision] : snapshot->FileSources) {
            ValidateFileSourceName(name.Underlying());
            THROW_ERROR_EXCEPTION_UNLESS(
                fileSourceRevision,
                "File snapshot source %Qv is null",
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

} // namespace NYT::NFlow
