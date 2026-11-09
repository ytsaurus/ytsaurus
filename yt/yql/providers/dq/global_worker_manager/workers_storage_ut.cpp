#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/dq/global_worker_manager/workers_storage.h>
#include <yt/yql/providers/dq/global_worker_manager/worker_filter.h>

using namespace NYql;
using namespace NYql::NDq;

Y_UNIT_TEST_SUITE(WorkersBenchmark) {
    Y_UNIT_TEST(RequestUploadLocationsSurviveDownloadReassignment) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        Yql::DqsProto::RegisterNodeRequest workerRequest;
        workerRequest.SetCapacity(1);
        workerRequest.AddKnownNodes(1);
        TGUID workerId;
        CreateGuid(&workerId);
        auto [worker, _] = storage.CreateOrUpdate(100, workerId, workerRequest);

        const TVector<std::pair<TString, TString>> locations = {
            {"cluster-one.yt", "//client"},
            {"cluster-two.yt", "//client"},
            {"", "//client"},
            {"cluster-one.yt", ""},
            {"", ""},
        };
        THashSet<NDqs::TDownloadKey, NDqs::TDownloadKeyHash> expected;
        for (bool perTask : {false, true}) {
            for (const auto& location : locations) {
                NDqProto::TAllocateWorkersRequest request;
                request.SetCount(1);
                request.SetUploadClusterAddress(location.first);
                request.SetUploadPrefix(location.second);
                auto* filter = perTask ? request.AddWorkerFilterPerTask() : nullptr;
                for (int i = 0; i < 2; ++i) {
                    auto* file = filter ? filter->AddFile() : request.AddFiles();
                    file->SetObjectId("resource-" + ToString(i));
                    file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
                    expected.insert({
                        .ObjectId = file->GetObjectId(),
                        .UploadPrefix = location.second,
                        .UploadClusterAddress = location.first,
                    });
                }
                IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
                UNIT_ASSERT(storage.TryAllocate(waitInfo).empty());
            }
        }

        auto checkDownloads = [&] (const NDqs::TWorkerInfo::TPtr& node) {
            const auto downloads = node->GetResourcesForDownloading();
            UNIT_ASSERT_VALUES_EQUAL(downloads.size(), expected.size());
            for (const auto& key : expected) {
                UNIT_ASSERT_VALUES_EQUAL(downloads.at(key).UploadClusterAddress, key.UploadClusterAddress);
                UNIT_ASSERT_VALUES_EQUAL(downloads.at(key).UploadPrefix, key.UploadPrefix);
                UNIT_ASSERT_VALUES_EQUAL(downloads.at(key).File.GetObjectId(), key.ObjectId);
            }
        };
        checkDownloads(worker);

        TGUID replacementId;
        CreateGuid(&replacementId);
        auto [replacement, unused] = storage.CreateOrUpdate(101, replacementId, workerRequest);
        Y_UNUSED(unused);
        worker->LastPingTime = TInstant::Now() - TDuration::Minutes(1);
        storage.CleanUp(TInstant::Now(), TDuration::Seconds(15));
        checkDownloads(replacement);
    }

    Y_UNIT_TEST(AlternativeSourcesHaveIndependentLimitsAndFinishTogether) {
        using namespace NDqs;
        auto metrics = MakeIntrusive<TSensorsGroup>();
        TGlobalResources resources(metrics);
        auto limiter = MakeIntrusive<TInflightLimiter>(1);
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(1);
        request.AddKnownNodes(1);
        TGUID workerId;
        CreateGuid(&workerId);
        auto worker = MakeIntrusive<TWorkerInfo>("start", 100u, workerId, request, metrics, limiter, resources);
        TGUID otherId;
        CreateGuid(&otherId);
        auto other = MakeIntrusive<TWorkerInfo>("start", 101u, otherId, request, metrics, limiter, resources);

        TWorkerInfo::TDownloadResource first{
            .UploadPrefix = "//A",
            .UploadClusterAddress = "cluster-one.yt",
        };
        first.File.SetObjectId("shared-file");
        UNIT_ASSERT(worker->AddToDownloadList(first));
        UNIT_ASSERT_VALUES_EQUAL(worker->GetResourcesForDownloading().size(), 1);
        UNIT_ASSERT(other->AddToDownloadList(first));
        UNIT_ASSERT(other->GetResourcesForDownloading().empty());

        auto second = first;
        second.UploadPrefix = "//B";
        auto third = second;
        third.UploadClusterAddress = "cluster-two.yt";
        UNIT_ASSERT(worker->AddToDownloadList(second));
        UNIT_ASSERT(worker->AddToDownloadList(third));
        UNIT_ASSERT(!worker->AddToDownloadList(first));
        UNIT_ASSERT_VALUES_EQUAL(worker->GetResourcesForDownloading().size(), 3);

        // A cached object clears pending sources without releasing another worker's slot.
        request.AddFilesOnNode()->SetObjectId("shared-file");
        other->Update(request);
        UNIT_ASSERT(other->GetDownloadList().empty());
        UNIT_ASSERT(!limiter->CanDownload(first.GetKey()));

        worker->Update(request);
        UNIT_ASSERT(worker->GetDownloadList().empty());
        UNIT_ASSERT(worker->GetActiveDownloads().empty());
        for (const auto& source : {first, second, third}) {
            UNIT_ASSERT(limiter->CanDownload(source.GetKey()));
            UNIT_ASSERT(!worker->AddToDownloadList(source));
        }
    }

    Y_UNIT_TEST(RssIsReportedInClusterStatus) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(1);
        request.AddKnownNodes(1);
        request.MutableRusage()->SetMaxRss(123456789);
        request.MutableRusage()->SetCurrentRss(98765432);
        storage.CreateOrUpdate(100, guid, request);

        Yql::DqsProto::ClusterStatusResponse response;
        storage.ClusterStatus(&response);

        UNIT_ASSERT_VALUES_EQUAL(response.WorkerSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetWorker(0).GetRusage().GetMaxRss(), 123456789);
        UNIT_ASSERT_VALUES_EQUAL(response.GetWorker(0).GetRusage().GetCurrentRss(), 98765432);
    }

    Y_UNIT_TEST(Basic) {
        int workers = 1000;
        int i;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(i+100, guid, request);
        }

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());

        TVector<NDqs::TWorkerInfo::TPtr> all;
        TInstant now = TInstant::Now();
        while (1) {
            NYql::NDqProto::TAllocateWorkersRequest request;
            request.SetCount(10);
            IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo);
            if (result.empty()) {
                break;
            }
            all.insert(all.end(), result.begin(), result.end());
        }

        Cerr << (TInstant::Now() - now) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(0, storage.FreeSlots());


        now = TInstant::Now();
        for (auto& node : all) {
            storage.FreeWorker(now, node);
        }
        Cerr << (TInstant::Now() - now) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());
    }

    Y_UNIT_TEST(AcquireAll) {
        int workers = 1;
        int i;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(i+100, guid, request);
        }

        UNIT_ASSERT_VALUES_EQUAL(workers*100, storage.FreeSlots());

        TVector<NDqs::TWorkerInfo::TPtr> all;
        while (1) {
            NYql::NDqProto::TAllocateWorkersRequest request;
            request.SetCount(10);
            IScheduler::TWaitInfo waitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo);
            if (result.empty()) {
                break;
            }
            all.insert(all.end(), result.begin(), result.end());
        }
        UNIT_ASSERT_VALUES_EQUAL(all.size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(0, storage.FreeSlots());
    }

    // Bounded mode must distribute tasks evenly across exactly workersCount workers.
    // Regresses the bug where tasksPerWorker was passed by value to ProcessMatched,
    // causing the per-worker cap to never trigger: all tasks landed on the first worker.
    Y_UNIT_TEST(BoundedModeDistribution) {
        const int Workers = 5;
        const int TasksPerWorker = 10;
        const int TotalTasks = Workers * TasksPerWorker;

        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        for (int i = 0; i < Workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest req;
            req.SetCapacity(1000); // high capacity — old bug: first worker would absorb all tasks
            req.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(100 + i, guid, req);
        }

        NYql::NDqProto::TAllocateWorkersRequest allocReq;
        allocReq.SetCount(TotalTasks);
        allocReq.SetWorkersCount(Workers);
        IScheduler::TWaitInfo waitInfo(allocReq, NActors::TActorId());
        auto result = storage.TryAllocate(waitInfo);

        UNIT_ASSERT_VALUES_EQUAL(result.size(), (size_t)TotalTasks);

        THashMap<TGUID, int> tasksPerWorker;
        for (const auto& w : result) {
            tasksPerWorker[w->WorkerId]++;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            tasksPerWorker.size(), (size_t)Workers,
            "All workers must be used in bounded mode");
        for (const auto& [_, count] : tasksPerWorker) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                count, TasksPerWorker,
                "Each worker must receive exactly TasksPerWorker tasks");
        }
    }

    Y_UNIT_TEST(ScheduleDownload) {
        int workers = 10;
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();
        for (int i = 0; i < workers; i++) {
            TGUID guid;
            Yql::DqsProto::RegisterNodeRequest request;
            request.SetCapacity(100);
            request.AddKnownNodes(1);
            CreateGuid(&guid);
            storage.CreateOrUpdate(100+i, guid, request);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            request.SetCount(10);

            auto waitInfo1 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo1);

            UNIT_ASSERT_VALUES_EQUAL(result.size(), 10);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            auto workerFilter = Yql::DqsProto::TWorkerFilter();
            workerFilter.AddNodeId(102);

            request.SetCount(10);
            for (ui32 i = 0; i < request.GetCount(); i++) {
                *request.AddWorkerFilterPerTask() = workerFilter;
            }
            auto waitInfo2 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo2);
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 10);
        }

        {
            auto request = NDqProto::TAllocateWorkersRequest();
            auto workerFilter = Yql::DqsProto::TWorkerFilter();
            workerFilter.AddNodeId(102);
            Yql::DqsProto::TFile file;
            file.SetObjectId("fileId");
            file.SetLocalPath("/tmp/test");
            *workerFilter.AddFile() = file;
            request.SetCount(10);
            for (ui32 i = 0; i < request.GetCount(); i++) {
                *request.AddWorkerFilterPerTask() = workerFilter;
            }

            auto waitInfo3 = IScheduler::TWaitInfo(request, NActors::TActorId());
            auto result = storage.TryAllocate(waitInfo3);
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 0);

            storage.Visit([](const NDqs::TWorkerInfo::TPtr& workerInfo) {
                UNIT_ASSERT(workerInfo->GetDownloadList().size() == 0 || workerInfo->NodeId == 102);
            });
        }
    }
}

Y_UNIT_TEST_SUITE(TWorkersStorageCheckZombieTest) {
    Y_UNIT_TEST(NewerWorkerMarksOldWorkerDead) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        const ui32 nodeId = 100;
        TGUID oldWorkerId;
        TGUID newWorkerId;
        CreateGuid(&oldWorkerId);
        CreateGuid(&newWorkerId);

        Yql::DqsProto::RegisterNodeRequest oldRequest;
        oldRequest.SetCapacity(1);
        oldRequest.AddKnownNodes(1);
        oldRequest.SetStartTime("2020-01-01T00:00:00.000000Z");
        storage.CreateOrUpdate(nodeId, oldWorkerId, oldRequest);

        Yql::DqsProto::RegisterNodeRequest newRequest;
        newRequest.SetCapacity(1);
        newRequest.AddKnownNodes(1);
        newRequest.SetStartTime("2021-01-01T00:00:00.000000Z");
        storage.CreateOrUpdate(nodeId, newWorkerId, newRequest);

        bool oldWorkerDead = false;
        bool newWorkerAlive = false;
        storage.Visit([&](const NDqs::TWorkerInfo::TPtr& workerInfo) {
            if (workerInfo->WorkerId == oldWorkerId) {
                oldWorkerDead = workerInfo->IsDead;
            }
            if (workerInfo->WorkerId == newWorkerId) {
                newWorkerAlive = !workerInfo->IsDead;
            }
        });

        UNIT_ASSERT(oldWorkerDead);
        UNIT_ASSERT(newWorkerAlive);
    }
}

Y_UNIT_TEST_SUITE(FileIndex) {
    // Build a 1-task allocation request requiring the given file IDs.
    static IScheduler::TWaitInfo MakeFileWaitInfo(int taskCount, TVector<TString> fileIds) {
        NYql::NDqProto::TAllocateWorkersRequest req;
        req.SetCount(taskCount);
        Yql::DqsProto::TWorkerFilter filter;
        for (const auto& id : fileIds) {
            auto* f = filter.AddFile();
            f->SetObjectId(id);
            f->SetLocalPath("/tmp/" + id);
        }
        for (int i = 0; i < taskCount; i++) {
            *req.AddWorkerFilterPerTask() = filter;
        }
        return IScheduler::TWaitInfo(req, NActors::TActorId());
    }

    // Build a RegisterNodeRequest reporting the given files as present on the node.
    static Yql::DqsProto::RegisterNodeRequest MakeWorkerReq(TVector<TString> fileIds, int capacity = 100) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapacity(capacity);
        req.AddKnownNodes(1);
        for (const auto& id : fileIds) {
            req.AddFilesOnNode()->SetObjectId(id);
        }
        return req;
    }

    // CreateOrUpdate requires a non-const lvalue reference; this wrapper avoids temporaries.
    static void AddWorker(TWorkersStorage& storage, ui32 nodeId, TGUID guid,
                          TVector<TString> fileIds, int capacity = 100) {
        auto req = MakeWorkerReq(std::move(fileIds), capacity);
        storage.CreateOrUpdate(nodeId, guid, req);
    }

    // Index is populated when a worker is created with files.
    Y_UNIT_TEST(PopulatedOnCreation) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1", "f2"});

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 1u, "worker with f1+f2 must be found via index");
    }

    // Index is updated when a worker gains new files (download completed).
    Y_UNIT_TEST(UpdatedOnFileAddition) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        // Worker starts with no files.
        AddWorker(storage, 101, guid, {});

        auto result1 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result1.size(), 0u, "no worker has f1 yet");

        // Worker reports f1 on next ping.
        AddWorker(storage, 101, guid, {"f1"});

        auto result2 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result2.size(), 1u, "worker now has f1, must be found");
    }

    // Index is updated when a worker loses files (eviction/restart).
    Y_UNIT_TEST(UpdatedOnFileRemoval) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1", "f2"});

        // Initial allocation for f1+f2 works.
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
            UNIT_ASSERT_VALUES_EQUAL(r.size(), 1u);
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // Worker loses f2 on next ping.
        AddWorker(storage, 101, guid, {"f1"});

        // f1+f2 must no longer be satisfiable.
        auto result2 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result2.size(), 0u, "f2 removed from worker, must not allocate");

        // f1 alone must still work.
        auto result3 = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result3.size(), 1u, "f1 still present, must allocate");
    }

    // Index is cleaned up when a worker is explicitly dropped.
    Y_UNIT_TEST(ClearedOnDropWorker) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1"});

        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
            UNIT_ASSERT_VALUES_EQUAL(r.size(), 1u);
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        storage.DropWorker(TInstant::Now(), guid);

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 0u, "worker dropped, index entry must be gone");
    }

    // Index is cleaned up when a worker expires during CleanUp.
    Y_UNIT_TEST(ClearedOnCleanUp) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        AddWorker(storage, 101, guid, {"f1"});

        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
            UNIT_ASSERT_VALUES_EQUAL(r.size(), 1u);
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // Expire the worker: pass a far-future "now" with zero tolerance.
        storage.CleanUp(TInstant::Now() + TDuration::Days(1), TDuration::Zero());

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 0u, "worker expired, index entry must be gone");
    }

    // Index is fully reset by Clear(), with no stale entries for new workers.
    Y_UNIT_TEST(ResetOnClear) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);

        // First population: worker with f1.
        {
            TGUID guid;
            CreateGuid(&guid);
            AddWorker(storage, 101, guid, {"f1"});
        }

        storage.Clear();

        // After Clear(), add a new worker without f1.
        {
            TGUID guid;
            CreateGuid(&guid);
            AddWorker(storage, 101, guid, {});
        }

        auto result = storage.TryAllocate(MakeFileWaitInfo(1, {"f1"}));
        UNIT_ASSERT_VALUES_EQUAL_C(result.size(), 0u, "stale index entry must be gone after Clear()");
    }

    // Index intersection selects only the worker that has ALL required files.
    Y_UNIT_TEST(IntersectionSelectsCorrectWorker) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID gA, gB, gC;
        CreateGuid(&gA); CreateGuid(&gB); CreateGuid(&gC);

        AddWorker(storage, 101, gA, {"f1", "f2"});       // A: f1, f2
        AddWorker(storage, 102, gB, {"f1", "f3"});       // B: f1, f3
        AddWorker(storage, 103, gC, {"f1", "f2", "f3"}); // C: f1, f2, f3

        // f1+f2: A or C only (not B — B lacks f2).
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "f1+f2 must match exactly one worker");
            UNIT_ASSERT_C(r[0]->WorkerId == gA || r[0]->WorkerId == gC,
                          "only A or C have both f1 and f2");
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // f1+f3: B or C only (not A — A lacks f3).
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f3"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "f1+f3 must match exactly one worker");
            UNIT_ASSERT_C(r[0]->WorkerId == gB || r[0]->WorkerId == gC,
                          "only B or C have both f1 and f3");
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // f1+f2+f3: only C has all three.
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"f1", "f2", "f3"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "f1+f2+f3 must match only C");
            UNIT_ASSERT_C(r[0]->WorkerId == gC, "only C has f1+f2+f3");
            storage.FreeWorker(TInstant::Now(), r[0]);
        }

        // Allocate 3 tasks requiring only f1: all three workers qualify.
        {
            auto r = storage.TryAllocate(MakeFileWaitInfo(3, {"f1"}));
            UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 3u, "all three workers have f1");
        }
    }

    // Index handles multiple workers sharing the same file correctly:
    // removing one worker must not evict others from the index entry.
    Y_UNIT_TEST(DropOneWorkerKeepsOthers) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID g1, g2;
        CreateGuid(&g1); CreateGuid(&g2);
        AddWorker(storage, 101, g1, {"shared"});
        AddWorker(storage, 102, g2, {"shared"});

        storage.DropWorker(TInstant::Now(), g1);

        // g2 still has the file and must be allocatable.
        auto r = storage.TryAllocate(MakeFileWaitInfo(1, {"shared"}));
        UNIT_ASSERT_VALUES_EQUAL_C(r.size(), 1u, "g2 still has 'shared', must be found");
        UNIT_ASSERT_C(r[0]->WorkerId == g2, "allocated worker must be g2");
    }
}

// Validate that TWorkerFilter::Match populates WaitingResources with the
// correct "task is waiting for a missing resource" semantics:
//   - worker LACKS the file  → taskId inserted (task is waiting)
//   - worker HAS   the file  → taskId erased   (task is no longer waiting)
// This regresses the bug where insert/erase were inverted, causing
// ResourceWaitTime stats to measure the opposite of waiting time.
Y_UNIT_TEST_SUITE(TWorkerFilterWaitingResources) {
    // Build a RegisterNodeRequest reporting the given files as present on the node.
    static Yql::DqsProto::RegisterNodeRequest MakeWorkerReq(TVector<TString> fileIds, int capacity = 100) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapacity(capacity);
        req.AddKnownNodes(1);
        for (const auto& id : fileIds) {
            req.AddFilesOnNode()->SetObjectId(id);
        }
        return req;
    }

    // Retrieve the single worker from storage via Visit.
    static NDqs::TWorkerInfo::TPtr GetSingleWorker(TWorkersStorage& storage) {
        NDqs::TWorkerInfo::TPtr result;
        storage.Visit([&](const NDqs::TWorkerInfo::TPtr& w) {
            if (!w->IsDead) {
                result = w;
            }
        });
        return result;
    }

    // Build a TWorkerFilter requiring the given file IDs.
    static TWorkerFilter MakeFilter(TVector<TString> fileIds) {
        Yql::DqsProto::TWorkerFilter proto;
        for (const auto& id : fileIds) {
            auto* f = proto.AddFile();
            f->SetObjectId(id);
            f->SetLocalPath("/tmp/" + id);
        }
        return TWorkerFilter(proto);
    }

    // Worker lacks the required file → taskId must be in WaitingResources.
    Y_UNIT_TEST(MissingFileInsertsTaskIntoWaitingResources) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        auto req = MakeWorkerReq({}); // worker has NO files
        storage.CreateOrUpdate(101, guid, req);

        auto worker = GetSingleWorker(storage);
        UNIT_ASSERT_C(worker, "worker must exist");

        auto filter = MakeFilter({"missing_file"});

        THashMap<TString, THashSet<int>> waitingResources;
        THashMap<TString, TResourceStat> uploaded;
        TWorkerFilter::TStats stats;
        stats.WaitingResources = &waitingResources;
        stats.Uploaded = &uploaded;

        const int taskId = 42;
        auto status = filter.Match(worker, taskId, &stats);

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(status), static_cast<int>(TWorkerFilter::EFAIL),
            "worker lacks the file, Match must return EFAIL");
        UNIT_ASSERT_C(
            waitingResources["missing_file"].contains(taskId),
            "taskId must be in WaitingResources when worker LACKS the file (task is waiting)");
    }

    // Worker has the required file → taskId must NOT be in WaitingResources.
    Y_UNIT_TEST(PresentFileErasesTaskFromWaitingResources) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        auto req = MakeWorkerReq({"present_file"}); // worker HAS the file
        storage.CreateOrUpdate(101, guid, req);

        auto worker = GetSingleWorker(storage);
        UNIT_ASSERT_C(worker, "worker must exist");

        auto filter = MakeFilter({"present_file"});

        THashMap<TString, THashSet<int>> waitingResources;
        THashMap<TString, TResourceStat> uploaded;
        TWorkerFilter::TStats stats;
        stats.WaitingResources = &waitingResources;
        stats.Uploaded = &uploaded;

        const int taskId = 42;
        // Pre-insert taskId to simulate a previous "waiting" state.
        waitingResources["present_file"].insert(taskId);

        auto status = filter.Match(worker, taskId, &stats);

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(status), static_cast<int>(TWorkerFilter::EOK),
            "worker has the file, Match must return EOK");
        UNIT_ASSERT_C(
            !waitingResources["present_file"].contains(taskId),
            "taskId must NOT be in WaitingResources when worker HAS the file (task is no longer waiting)");
    }

    // Mixed: worker has one file but lacks another.
    // The missing file must have taskId in WaitingResources; the present one must not.
    Y_UNIT_TEST(MixedFilesWaitingResources) {
        TWorkersStorage storage(1, new TSensorsGroup, new TSensorsGroup);
        storage.Clear();

        TGUID guid;
        CreateGuid(&guid);
        auto req = MakeWorkerReq({"have_it"}); // worker has "have_it" but not "need_it"
        storage.CreateOrUpdate(101, guid, req);

        auto worker = GetSingleWorker(storage);
        UNIT_ASSERT_C(worker, "worker must exist");

        auto filter = MakeFilter({"have_it", "need_it"});

        THashMap<TString, THashSet<int>> waitingResources;
        THashMap<TString, TResourceStat> uploaded;
        TWorkerFilter::TStats stats;
        stats.WaitingResources = &waitingResources;
        stats.Uploaded = &uploaded;

        const int taskId = 7;
        // Pre-insert taskId for "have_it" to verify it gets erased.
        waitingResources["have_it"].insert(taskId);

        auto status = filter.Match(worker, taskId, &stats);

        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(status), static_cast<int>(TWorkerFilter::EFAIL),
            "worker lacks 'need_it', Match must return EFAIL");
        UNIT_ASSERT_C(
            !waitingResources["have_it"].contains(taskId),
            "taskId must NOT be in WaitingResources for 'have_it' (worker has it)");
        UNIT_ASSERT_C(
            waitingResources["need_it"].contains(taskId),
            "taskId must be in WaitingResources for 'need_it' (worker lacks it)");
    }
}
