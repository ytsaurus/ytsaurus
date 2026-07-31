#include "file_source.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TFileSourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("file_source_class_name", &TThis::FileSourceClassName)
        .NonEmpty();
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TFileSourceRevision::Register(TRegistrar registrar)
{
    registrar.Parameter("file_source_class_name", &TThis::FileSourceClassName)
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
            "File source revision object id must be nonempty");
        THROW_ERROR_EXCEPTION_IF(
            revision->Size && *revision->Size < 0,
            "File source revision size must be nonnegative");
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
