#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/chunk_detail.h>
#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/server/node/exec_node/volume_manager.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/testing/common/env.h>

#include <util/system/env.h>
#include <util/system/fs.h>

namespace NYT::NDataNode {
namespace {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NExecNode;
using namespace NDataNode;
using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMockChunkCache)

class TMockChunkCache
    : public IVolumeChunkCache
{
public:
    MOCK_METHOD(TFuture<IVolumeArtifactPtr>, DownloadArtifact, (
        const NDataNode::TArtifactKey& ,
        const TArtifactDownloadOptions& ));
};

DEFINE_REFCOUNTED_TYPE(TMockChunkCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFakeArtifact)

struct TFakeArtifact
    : public IVolumeArtifact
{
public:
    TFakeArtifact(const TString& filename)
        : Filename(filename)
    { }

    TString Filename;

    TString GetFileName() const override
    {
        return Filename;
    }
};

DEFINE_REFCOUNTED_TYPE(TFakeArtifact)

////////////////////////////////////////////////////////////////////////////////

struct TTestOptions
{
    bool UseSquashfs = false;
};

class TVolumeManagerTest
    : public ::testing::TestWithParam<TTestOptions>
{
public:
    const TActionQueuePtr Thread = New<TActionQueue>("VolumeManagerTest");

    const TDataNodeConfigPtr Config = New<TDataNodeConfig>();
    const TClusterNodeDynamicConfigPtr DynamicConfig = New<TClusterNodeDynamicConfig>();
    const TClusterNodeDynamicConfigManagerPtr DynamicConfigManager = New<TClusterNodeDynamicConfigManager>(DynamicConfig);

    TMockChunkCachePtr MockChunkCache = New<TMockChunkCache>();

    IVolumeManagerPtr VolumeManager;

    TString OldPath = GetEnv("PATH", "");

    void SetUp() override
    {
        SetEnv("PATH", OldPath + ":" + BinaryPath("yt/yt/server/tools/bin"));

        testing::Mock::AllowLeak(MockChunkCache.Get());

        auto layerLocation = New<TLayerLocationConfig>();
        auto testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
        layerLocation->Path = GetOutputPath() / testInfo->test_suite_name() / testInfo->name() / "location";
        layerLocation->LocationIsAbsolute = false;
        layerLocation->Postprocess();

        Config->VolumeManager->LayerLocations.push_back(layerLocation);
        Config->VolumeManager->ConvertLayersToSquashfs = GetParam().UseSquashfs;
        Config->VolumeManager->Tar2SquashToolPath = BinaryPath("yt/go/tar2squash/tar2squash");
        Config->Postprocess();

        DynamicConfig->Postprocess();

        BIND([this] {
            VolumeManager = WaitFor(CreatePortoVolumeManager(
                Config,
                DynamicConfigManager,
                MockChunkCache,
                Thread->GetInvoker(),
                GetNullMemoryUsageTracker(),
                nullptr))
                .ValueOrThrow();
        })
            .AsyncVia(Thread->GetInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void TearDown() override
    {
        VolumeManager->ClearCaches();
        Sleep(TDuration::Seconds(1));

        SetEnv("PATH", OldPath);

        testing::Mock::VerifyAndClear(MockChunkCache.Get());
    }

    TArtifactKey FixupKey(TArtifactKey key)
    {
        if (GetParam().UseSquashfs) {
            key.set_is_squashfs_image(true);
        }

        return key;
    }

    auto ReturnFakeArtifact(const TString& filename)
    {
        return [filename, this] (
            const NDataNode::TArtifactKey& ,
            const TArtifactDownloadOptions& artifactDownloadOptions
        ) {
            return BIND([filename, artifactDownloadOptions] () {
                auto arcadiaPath = ArcadiaSourceRoot() + "/yt/yt/server/node/data_node/unittests/testdata/" + filename;

                auto newName = GetWorkPath() + "/" + ToString(TGuid::Create());
                TFileInput originalFile(arcadiaPath);
                if (auto converter = artifactDownloadOptions.Converter) {
                    converter(CreateZeroCopyAdapter(CreateAsyncAdapter(&originalFile)), newName);
                } else {
                    TFileOutput fileCopy(newName);
                    originalFile.ReadAll(fileCopy);
                    fileCopy.Finish();
                }

                return IVolumeArtifactPtr{New<TFakeArtifact>(newName)};
            })
                .AsyncVia(Thread->GetInvoker())
                .Run();
        };
    }
};

TEST_P(TVolumeManagerTest, SetUp)
{ }

TEST_P(TVolumeManagerTest, CreateSimpleVolume)
{
    EXPECT_CALL(*MockChunkCache, DownloadArtifact(_, _))
        .WillOnce(ReturnFakeArtifact("base.tgz"));

    std::vector<TArtifactKey> layers{};
    layers.emplace_back();

    TArtifactDownloadOptions options;

    auto volume = WaitFor(VolumeManager->PrepareVolume(layers, options, {}))
        .ValueOrThrow();

    EXPECT_TRUE(TFileStat(volume->GetPath() + "/bar").IsFile());
    EXPECT_TRUE(TFileStat(volume->GetPath() + "/foo").IsDir());
    EXPECT_TRUE(TFileStat(volume->GetPath() + "/foo/zig").IsFile());

    WaitFor(volume->Remove())
        .ThrowOnError();
}

TEST_P(TVolumeManagerTest, TwoLayers)
{
    TArtifactKey baseKey;
    ToProto(baseKey.add_chunk_specs()->mutable_chunk_id(), TGuid::Create());
    baseKey = FixupKey(baseKey);

    TArtifactKey upperKey;
    ToProto(upperKey.add_chunk_specs()->mutable_chunk_id(), TGuid::Create());
    upperKey = FixupKey(upperKey);

    EXPECT_CALL(*MockChunkCache, DownloadArtifact(baseKey, _))
        .WillOnce(ReturnFakeArtifact("base.tgz"));

    EXPECT_CALL(*MockChunkCache, DownloadArtifact(upperKey, _))
        .WillOnce(ReturnFakeArtifact("upper.tgz"));

    std::vector<TArtifactKey> layers{upperKey, baseKey};
    TArtifactDownloadOptions options;

    auto volume = WaitFor(VolumeManager->PrepareVolume(layers, options, {}))
        .ValueOrThrow();

    EXPECT_FALSE(NFs::Exists(volume->GetPath() + "/bar"));
    EXPECT_TRUE(TFileStat(volume->GetPath() + "/foo").IsDir());
    EXPECT_TRUE(TFileStat(volume->GetPath() + "/foo/zog").IsFile());
    EXPECT_FALSE(NFs::Exists(volume->GetPath() + "/foo/zig"));

    WaitFor(volume->Remove())
        .ThrowOnError();
}

TEST_P(TVolumeManagerTest, CreateRootFsVolumeWithQuota)
{
    TArtifactKey baseKey;
    ToProto(baseKey.add_chunk_specs()->mutable_chunk_id(), TGuid::Create());
    baseKey = FixupKey(baseKey);

    TArtifactKey upperKey;
    ToProto(upperKey.add_chunk_specs()->mutable_chunk_id(), TGuid::Create());
    upperKey = FixupKey(upperKey);

    EXPECT_CALL(*MockChunkCache, DownloadArtifact(baseKey, _))
        .WillOnce(ReturnFakeArtifact("base.tgz"));

    EXPECT_CALL(*MockChunkCache, DownloadArtifact(upperKey, _))
        .WillOnce(ReturnFakeArtifact("upper.tgz"));

    std::vector<TArtifactKey> layers{upperKey, baseKey};
    TArtifactDownloadOptions options;

    auto volume = WaitFor(VolumeManager->PrepareVolume(layers, options, TUserSandboxOptions{
        .InodeLimit = 200L,
        .DiskSpaceLimit = 2 << 20,
        .HasRootFsQuota = true,
        .EnableDiskQuota = false
    }))
        .ValueOrThrow();

    EXPECT_FALSE(NFs::Exists(volume->GetPath() + "/bar"));
    EXPECT_TRUE(TFileStat(volume->GetPath() + "/foo").IsDir());
    EXPECT_TRUE(TFileStat(volume->GetPath() + "/foo/zog").IsFile());
    EXPECT_FALSE(NFs::Exists(volume->GetPath() + "/foo/zig"));

    WaitFor(volume->Remove())
        .ThrowOnError();
}

INSTANTIATE_TEST_SUITE_P(
    TestDefaultConfiguration,
    TVolumeManagerTest,
    ::testing::Values(TTestOptions{}));

INSTANTIATE_TEST_SUITE_P(
    TestSquashFS,
    TVolumeManagerTest,
    ::testing::Values(TTestOptions{.UseSquashfs = true}));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
