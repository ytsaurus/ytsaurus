#include "local_file_provider.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_STRUCT(TLocalFileProviderLocator);

struct TLocalFileProviderLocator
    : public TYsonStruct
{
    std::string Path;
    std::string Basename;

    REGISTER_YSON_STRUCT(TLocalFileProviderLocator);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path);
        registrar.Parameter("basename", &TThis::Basename);
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalFileProviderLocator);

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TLocalFileProviderParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
}

TFuture<TFileProviderRevisionPtr> TLocalFileProvider::Discover()
{
    const auto& path = GetParameters()->Path;
    auto basename = std::string(TFsPath(path).Basename());
    ValidateFileProviderName(basename);

    auto locator = New<TLocalFileProviderLocator>();
    locator->Path = path;
    locator->Basename = basename;

    auto revision = New<TFileProviderRevision>();
    revision->FileProviderClassName = TypeName<TLocalFileProvider>();
    revision->ObjectId = NFileStorage::TFileStorageObjectId(
        Format("local_file:v1:%v", path));
    revision->DisplayVersion = path;
    revision->Locator = ConvertToNode(locator)->AsMap();
    return MakeFuture<TFileProviderRevisionPtr>(std::move(revision));
}

TFuture<void> TLocalFileProvider::Download(
    const TFileProviderRevisionPtr& revision,
    const std::string& stagingDirectory)
{
    auto locator = ConvertTo<TLocalFileProviderLocatorPtr>(revision->Locator);
    ValidateFileProviderName(locator->Basename);
    auto provider = TFsPath(locator->Path);
    THROW_ERROR_EXCEPTION_UNLESS(
        provider.IsFile() && !provider.IsSymlink(),
        "Local file provider path %Qv is not a regular file",
        locator->Path);
    // TODO(mikari): Try a copy-on-write filesystem clone before falling back to copying.
    provider.CopyTo((TFsPath(stagingDirectory) / locator->Basename).GetPath(), false);
    return MakeFuture<void>(TError());
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_FILE_PROVIDER(TLocalFileProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
