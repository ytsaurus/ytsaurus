#include "local_file_source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_STRUCT(TLocalFileSourceLocator);

struct TLocalFileSourceLocator
    : public TYsonStruct
{
    std::string Path;
    std::string Basename;

    REGISTER_YSON_STRUCT(TLocalFileSourceLocator);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);
        registrar.Parameter("basename", &TThis::Basename);
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalFileSourceLocator);

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TLocalFileSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
}

TFuture<TFileSourceRevisionPtr> TLocalFileSource::Discover()
{
    const auto& path = GetParameters()->Path;
    auto basename = std::string(TFsPath(path).Basename());
    ValidateFileSourceName(basename);

    auto locator = New<TLocalFileSourceLocator>();
    locator->Path = path;
    locator->Basename = basename;

    auto revision = New<TFileSourceRevision>();
    revision->FileSourceClassName = TypeName<TLocalFileSource>();
    revision->ObjectId = NFileStorage::TFileStorageObjectId(
        Format("local_file:v1:%v", path));
    revision->DisplayVersion = path;
    revision->Locator = ConvertToNode(locator)->AsMap();
    return MakeFuture<TFileSourceRevisionPtr>(std::move(revision));
}

TFuture<void> TLocalFileSource::Download(
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    auto locator = ConvertTo<TLocalFileSourceLocatorPtr>(revision->Locator);
    ValidateFileSourceName(locator->Basename);
    auto source = TFsPath(locator->Path);
    THROW_ERROR_EXCEPTION_UNLESS(
        source.IsFile() && !source.IsSymlink(),
        "Local file source path %Qv is not a regular file",
        locator->Path);
    // TODO(mikari): Try a copy-on-write filesystem clone before falling back to copying.
    source.CopyTo((TFsPath(stagingDirectory) / locator->Basename).GetPath(), false);
    return MakeFuture<void>(TError());
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_FILE_SOURCE(TLocalFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
