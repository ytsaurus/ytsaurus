#include <yt/yql/providers/dq/testlib/test_coordination_helper.h>

#include <yt/yql/providers/dq/actors/yt/resource_manager.h>
#include <yt/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <yt/yql/providers/dq/runtime/file_cache.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYql {
    namespace {

        class TDownloadCoordinator: public TTestCoordinationHelper {
        public:
            TDownloadCoordinator(ui32 nodeId, NActors::TActorId wrapper)
                : TTestCoordinationHelper(nodeId)
                , Wrapper(wrapper)
            {
            }

            NActors::TActorId GetWrapper(
                NActors::TActorSystem*, const TString&,
                const TString&, const TString&) override {
                return Wrapper;
            }

            const NActors::TActorId Wrapper;
        };

        void CheckAlternativeSource(bool firstSucceeds) {
            using namespace NActors;
            TTempDir tempDir;
            TTestActorRuntimeBase runtime;
            runtime.Initialize();
            const auto wrapper = runtime.AllocateEdgeActor();
            const auto parent = runtime.AllocateEdgeActor();
            auto coordinator = MakeIntrusive<TDownloadCoordinator>(runtime.GetNodeId(0), wrapper);
            TResourceManagerOptions options;
            options.UploadClusterAddress = "cluster.yt";
            options.YtBackend.SetUser("worker-user");
            options.UploadPrefix = "//A";
            options.MaxRetries = -1;
            options.TmpDir = tempDir.Name() + "/download";
            options.FileCache = MakeIntrusive<TFileCache>(tempDir.Name() + "/cache", 1024);
            TResourceFile file;
            file.LocalFileName = "shared-file";
            file.RemoteFileName = "udfs/shared-file";
            options.Files.push_back(file);

            const auto first = runtime.Register(CreateResourceDownloader(options, coordinator),
                                                0, 0, TMailboxType::Simple, 0, parent);
            const auto readA = runtime.GrabEdgeEvent<TEvReadFile>(wrapper, TDuration::Seconds(5));
            UNIT_ASSERT(readA);
            options.UploadPrefix = "//B";
            const auto second = runtime.Register(CreateResourceDownloader(options, coordinator),
                                                 0, 0, TMailboxType::Simple, 0, parent);
            const auto readB = runtime.GrabEdgeEvent<TEvReadFile>(wrapper, TDuration::Seconds(5));
            UNIT_ASSERT(readB);
            const auto& pathA = std::get<1>(*readA->Get());
            const auto& pathB = std::get<1>(*readB->Get());
            UNIT_ASSERT(pathA != pathB);
            for (const auto& path : {pathA, pathB}) {
                TFsPath(path).Parent().MkDirs();
                TFileOutput output(path);
                output << "content";
            }
            if (!firstSucceeds) {
                runtime.Send(new IEventHandle(first, wrapper,
                    new TEvReadFileResponse(readA->Get()->RequestId, NYT::TError("source A unavailable"))));
            }
            runtime.Send(new IEventHandle(second, wrapper,
                new TEvReadFileResponse(readB->Get()->RequestId, NYT::TErrorOr<void>())));
            TAutoPtr<IEventHandle> completeBHandle;
            const auto* completeB = runtime.GrabEdgeEvent<TEvDownloadComplete>(completeBHandle, TDuration::Seconds(5));
            UNIT_ASSERT(completeB);
            UNIT_ASSERT(completeBHandle->Sender == second);
            UNIT_ASSERT(completeB->FailedObjectIds.empty());
            UNIT_ASSERT(options.FileCache->Contains(file.LocalFileName));

            if (firstSucceeds) {
                runtime.Send(new IEventHandle(first, wrapper,
                    new TEvReadFileResponse(readA->Get()->RequestId, NYT::TErrorOr<void>())));
            } else {
                // Retry after B succeeded must finish without issuing another YT read.
                runtime.Send(new IEventHandle(first, parent, new TEvTick()));
            }
            TAutoPtr<IEventHandle> completeAHandle;
            const auto* completeA = runtime.GrabEdgeEvent<TEvDownloadComplete>(completeAHandle, TDuration::Seconds(5));
            UNIT_ASSERT(completeA);
            UNIT_ASSERT(completeAHandle->Sender == first);
            UNIT_ASSERT(completeA->FailedObjectIds.empty());
            const auto cached = options.FileCache->FindFile(file.LocalFileName);
            UNIT_ASSERT(cached);
            UNIT_ASSERT_VALUES_EQUAL(TFileInput(*cached).ReadAll(), "content");
            size_t count = 0;
            options.FileCache->Walk([&] (const TString&) { ++count; });
            UNIT_ASSERT_VALUES_EQUAL(count, 1);
            UNIT_ASSERT_VALUES_EQUAL(options.FileCache->UsedDiskSize(), 7);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TResourceDownloaderTest) {
        Y_UNIT_TEST(SkipsRetryAfterAnotherSourceSucceeded) {
            CheckAlternativeSource(false);
        }

        Y_UNIT_TEST(ConcurrentSourcesUseSeparateFilesAndOneCacheEntry) {
            CheckAlternativeSource(true);
        }
    } // Y_UNIT_TEST_SUITE(TResourceDownloaderTest)

} // namespace NYql
