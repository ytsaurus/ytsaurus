#include "file_provider.h"

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ValidateFileProviderName(TStringBuf name)
{
    const auto path = std::string(name);
    THROW_ERROR_EXCEPTION_UNLESS(
        NFS::IsPathRelativeAndInvolvesNoTraversal(path) &&
            !name.empty() &&
            name != "." &&
            name != ".." &&
            name.find('/') == TStringBuf::npos &&
            name.find('\\') == TStringBuf::npos &&
            name.find('\0') == TStringBuf::npos,
        "File provider name %Qv must be a single normal path component",
        name);
}

////////////////////////////////////////////////////////////////////////////////

void TFileProviderSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("file_provider_class_name", &TThis::FileProviderClassName)
        .NonEmpty();
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
    registrar.Parameter("postprocess_command", &TThis::PostprocessCommand)
        .Default();
    registrar.Parameter("postprocess_timeout", &TThis::PostprocessTimeout)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Minutes(1));
    registrar.Postprocessor([] (TThis* spec) {
        THROW_ERROR_EXCEPTION_IF(
            spec->PostprocessCommand && spec->PostprocessCommand->empty(),
            "File provider postprocess command must be nonempty");
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicFileProviderSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TFileProviderRevision::Register(TRegistrar registrar)
{
    registrar.Parameter("file_provider_class_name", &TThis::FileProviderClassName)
        .NonEmpty();
    registrar.Parameter("object_id", &TThis::ObjectId);
    registrar.Parameter("display_version", &TThis::DisplayVersion)
        .Default();
    registrar.Parameter("size", &TThis::Size)
        .Default();
    registrar.Parameter("locator", &TThis::Locator)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
    registrar.Postprocessor([] (TThis* revision) {
        THROW_ERROR_EXCEPTION_IF(
            revision->ObjectId.Underlying().empty(),
            "File provider revision object id must be nonempty");
        THROW_ERROR_EXCEPTION_IF(
            revision->Size && *revision->Size < 0,
            "File provider revision size must be nonnegative");
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
