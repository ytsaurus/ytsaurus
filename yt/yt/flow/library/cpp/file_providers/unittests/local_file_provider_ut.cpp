#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/file_providers/local_file_provider.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TLocalFileProviderPtr MakeProvider(const std::string& path)
{
    auto parameters = New<TLocalFileProviderParameters>();
    parameters->Path = path;

    auto spec = New<TFileProviderSpec>();
    spec->FileProviderClassName = TypeName<TLocalFileProvider>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TFileProviderContext>();
    context->ProviderSpec = std::move(spec);

    auto dynamicSpec = New<TDynamicFileProviderSpec>();
    dynamicSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    auto dynamicContext = New<TDynamicFileProviderContext>();
    dynamicContext->DynamicFileProviderSpec = std::move(dynamicSpec);
    return New<TLocalFileProvider>(std::move(context), std::move(dynamicContext));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLocalFileProviderTest, DiscoveryDoesNotReadAndIsDeterministic)
{
    TTempDir root;
    auto path = (TFsPath(root.Name()) / "missing.txt").GetPath();
    auto provider = MakeProvider(path);

    auto first = WaitFor(provider->Discover()).ValueOrThrow();
    auto second = WaitFor(provider->Discover()).ValueOrThrow();

    EXPECT_EQ(first->FileProviderClassName, TypeName<TLocalFileProvider>());
    EXPECT_EQ(first->ObjectId, second->ObjectId);
    EXPECT_EQ(first->ObjectId.Underlying(), Format("local_file:v1:%v", path));
    EXPECT_EQ(first->DisplayVersion, path);
    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("path"), path);
    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("basename"), "missing.txt");
}

TEST(TLocalFileProviderTest, DownloadsExactBytesUnderBasename)
{
    TTempDir root;
    auto inputPath = TFsPath(root.Name()) / "input.bin";
    {
        TOFStream output(inputPath.GetPath());
        output << "first\nsecond";
        output.Finish();
    }

    auto provider = MakeProvider(inputPath.GetPath());
    auto revision = WaitFor(provider->Discover()).ValueOrThrow();
    auto staging = TFsPath(root.Name()) / "staging";
    staging.MkDirs();
    WaitFor(provider->Download(revision, staging.GetPath())).ThrowOnError();

    TFileInput input((staging / "input.bin").GetPath());
    EXPECT_EQ(input.ReadAll(), "first\nsecond");
}

TEST(TLocalFileProviderTest, InPlaceChangesDoNotChangeRevision)
{
    TTempDir root;
    auto inputPath = TFsPath(root.Name()) / "immutable.txt";
    inputPath.Touch();
    auto provider = MakeProvider(inputPath.GetPath());
    auto first = WaitFor(provider->Discover()).ValueOrThrow();

    {
        TOFStream output(inputPath.GetPath());
        output << "changed";
        output.Finish();
    }
    auto second = WaitFor(provider->Discover()).ValueOrThrow();

    EXPECT_EQ(first->ObjectId, second->ObjectId);
}

TEST(TLocalFileProviderTest, RejectsMalformedSerializedBasenameAtDownload)
{
    TTempDir root;
    auto inputPath = TFsPath(root.Name()) / "input.bin";
    inputPath.Touch();
    auto provider = MakeProvider(inputPath.GetPath());
    auto revision = WaitFor(provider->Discover()).ValueOrThrow();
    revision->Locator = ConvertTo<IMapNodePtr>(TYsonString(Format("{path=%Qv;basename=\"../escaped\";}",
        inputPath.GetPath())));
    auto staging = TFsPath(root.Name()) / "staging";
    staging.MkDirs();

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(provider->Download(revision, staging.GetPath())).ThrowOnError(),
        "single normal path component");
    EXPECT_FALSE((TFsPath(root.Name()) / "escaped").Exists());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
