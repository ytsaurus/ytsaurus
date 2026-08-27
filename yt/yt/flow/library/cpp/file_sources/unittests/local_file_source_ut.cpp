#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/file_sources/local_file_source.h>

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

TLocalFileSourcePtr MakeSource(const std::string& path)
{
    auto parameters = New<TLocalFileSourceParameters>();
    parameters->Path = path;

    auto spec = New<TFileSourceSpec>();
    spec->FileSourceClassName = TypeName<TLocalFileSource>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();

    auto context = New<TFileSourceContext>();
    context->SourceSpec = std::move(spec);

    auto dynamicSpec = New<TDynamicFileSourceSpec>();
    dynamicSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    auto dynamicContext = New<TDynamicFileSourceContext>();
    dynamicContext->DynamicFileSourceSpec = std::move(dynamicSpec);
    return New<TLocalFileSource>(std::move(context), std::move(dynamicContext));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLocalFileSourceTest, DiscoveryDoesNotReadAndIsDeterministic)
{
    TTempDir root;
    auto path = (TFsPath(root.Name()) / "missing.txt").GetPath();
    auto source = MakeSource(path);

    auto first = WaitFor(source->Discover()).ValueOrThrow();
    auto second = WaitFor(source->Discover()).ValueOrThrow();

    EXPECT_EQ(first->FileSourceClassName, TypeName<TLocalFileSource>());
    EXPECT_EQ(first->ObjectId, second->ObjectId);
    EXPECT_EQ(first->ObjectId.Underlying(), Format("local_file:v1:%v", path));
    EXPECT_EQ(first->DisplayVersion, path);
    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("path"), path);
    EXPECT_EQ(first->Locator->GetChildValueOrThrow<std::string>("basename"), "missing.txt");
}

TEST(TLocalFileSourceTest, DownloadsExactBytesUnderBasename)
{
    TTempDir root;
    auto sourcePath = TFsPath(root.Name()) / "input.bin";
    {
        TOFStream output(sourcePath.GetPath());
        output << "first\nsecond";
        output.Finish();
    }

    auto source = MakeSource(sourcePath.GetPath());
    auto revision = WaitFor(source->Discover()).ValueOrThrow();
    auto staging = TFsPath(root.Name()) / "staging";
    staging.MkDirs();
    WaitFor(source->Download(revision, staging.GetPath())).ThrowOnError();

    TFileInput input((staging / "input.bin").GetPath());
    EXPECT_EQ(input.ReadAll(), "first\nsecond");
}

TEST(TLocalFileSourceTest, InPlaceChangesDoNotChangeRevision)
{
    TTempDir root;
    auto sourcePath = TFsPath(root.Name()) / "immutable.txt";
    sourcePath.Touch();
    auto source = MakeSource(sourcePath.GetPath());
    auto first = WaitFor(source->Discover()).ValueOrThrow();

    {
        TOFStream output(sourcePath.GetPath());
        output << "changed";
        output.Finish();
    }
    auto second = WaitFor(source->Discover()).ValueOrThrow();

    EXPECT_EQ(first->ObjectId, second->ObjectId);
}

TEST(TLocalFileSourceTest, RejectsMalformedSerializedBasenameAtDownload)
{
    TTempDir root;
    auto sourcePath = TFsPath(root.Name()) / "input.bin";
    sourcePath.Touch();
    auto source = MakeSource(sourcePath.GetPath());
    auto revision = WaitFor(source->Discover()).ValueOrThrow();
    revision->Locator = ConvertTo<IMapNodePtr>(TYsonString(Format("{path=%Qv;basename=\"../escaped\";}",
        sourcePath.GetPath())));
    auto staging = TFsPath(root.Name()) / "staging";
    staging.MkDirs();

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(source->Download(revision, staging.GetPath())).ThrowOnError(),
        "single normal path component");
    EXPECT_FALSE((TFsPath(root.Name()) / "escaped").Exists());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
