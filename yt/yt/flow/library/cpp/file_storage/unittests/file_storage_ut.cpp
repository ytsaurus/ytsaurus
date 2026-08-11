#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>
#include <yt/yt/flow/library/cpp/file_storage/private.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fstat.h>
#include <util/system/sysstat.h>

#include <cerrno>

namespace NYT::NFlow::NFileStorage {
namespace {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TStorageFixture
{
    TTempDir Root;
    TActionQueuePtr Queue = New<TActionQueue>();
    IStatusProfilerPtr StatusProfiler = CreateSyncStatusProfiler();

    ~TStorageFixture()
    {
        MakeWritable(TFsPath(Root.Name()));
    }

    TFileStorageConfigPtr MakeConfig(i64 softLimit = 1_MB, i64 hardLimit = 2_MB) const
    {
        auto config = New<TFileStorageConfig>();
        config->Path = Root.Name();
        config->SoftSizeLimit = softLimit;
        config->HardSizeLimit = hardLimit;
        config->CleanupPeriod = TDuration::Hours(1);
        return config;
    }

    IFileStoragePtr MakeStorage(
        i64 softLimit = 1_MB,
        i64 hardLimit = 2_MB) const
    {
        return CreateFileStorage(
            MakeConfig(softLimit, hardLimit),
            Queue->GetInvoker(),
            NLogging::TLogger("FileStorageTest"),
            {},
            StatusProfiler);
    }

    bool HasStatusError(TStringBuf path) const
    {
        return StatusProfiler->GetStatus().Errors.contains(std::string(path));
    }

private:
    static void MakeWritable(const TFsPath& path)
    {
        if (!path.Exists()) {
            return;
        }
        TFileStat stat(path, /*nofollow*/ true);
        THROW_ERROR_EXCEPTION_UNLESS(
            Chmod(path.GetPath().c_str(), stat.Mode | S_IWUSR) == 0,
            "Failed to make test file storage entry writable")
            .With(TError::FromSystem());
        if (stat.IsDir()) {
            TVector<TFsPath> children;
            path.List(children);
            for (const auto& child : children) {
                MakeWritable(child);
            }
        }
    }
};

TFileStorageFiller MakeFiller(std::string value, int* fillCount)
{
    return [value = std::move(value), fillCount] (const std::string& path) {
        ++*fillCount;
        TOFStream output((TFsPath(path) / "data").GetPath());
        output << value;
        output.Finish();
        return MakeFuture<void>(TError());
    };
}

std::string ReadPayload(const IFileStorageObjectPtr& object)
{
    return TFileInput((TFsPath(object->GetPath()) / "data").GetPath()).ReadAll();
}

void ChangePermissions(const TFsPath& path, int mode)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Chmod(path.GetPath().c_str(), mode) == 0,
        "Failed to change test path permissions")
        .With(TError::FromSystem());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFileStorageConfigTest, ValidatesPathAndLimits)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TFileStorageConfigPtr>(TYsonString(TStringBuf(
            "{path=relative;soft_size_limit=1;hard_size_limit=2;}"))),
        "must be absolute");
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TFileStorageConfigPtr>(TYsonString(TStringBuf(
            "{path=\"/tmp/cache\";soft_size_limit=3;hard_size_limit=2;}"))),
        "must not exceed");
}

TEST(TFileStorageTest, SharesFillAndReusesAcrossRestart)
{
    TStorageFixture fixture;
    int fillCount = 0;
    auto storage = fixture.MakeStorage();
    auto id = TFileStorageObjectId("test:v1:shared");

    auto first = WaitFor(storage->GetOrCreate(id, MakeFiller("payload", &fillCount))).ValueOrThrow();
    auto second = WaitFor(storage->GetOrCreate(id, MakeFiller("other", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 1);
    EXPECT_EQ(first->GetPath(), second->GetPath());
    EXPECT_EQ(ReadPayload(second), "payload");

    first.Reset();
    second.Reset();
    storage.Reset();
    storage = fixture.MakeStorage();
    auto restarted = WaitFor(storage->GetOrCreate(id, MakeFiller("other", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 1);
    EXPECT_EQ(ReadPayload(restarted), "payload");
}

TEST(TFileStorageTest, ConcurrentRequestsShareOneInflightFill)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage();
    auto started = NewPromise<void>();
    auto gate = NewPromise<void>();
    int fillCount = 0;
    auto filler = [&] (const std::string& path) {
        ++fillCount;
        TOFStream output((TFsPath(path) / "data").GetPath());
        output << "payload";
        output.Finish();
        started.Set();
        return gate.ToFuture();
    };

    auto first = storage->GetOrCreate(TFileStorageObjectId("test:v1:concurrent"), filler);
    WaitFor(started.ToFuture()).ThrowOnError();
    auto second = storage->GetOrCreate(TFileStorageObjectId("test:v1:concurrent"), filler);
    gate.Set();

    EXPECT_TRUE(WaitFor(first).IsOK());
    EXPECT_TRUE(WaitFor(second).IsOK());
    EXPECT_EQ(fillCount, 1);
}

TEST(TFileStorageTest, CallerCancellationDoesNotCancelSharedFill)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage();
    auto started = NewPromise<void>();
    auto gate = NewPromise<void>();
    int fillCount = 0;
    auto id = TFileStorageObjectId("test:v1:cancel-isolation");

    auto first = storage->GetOrCreate(
        id,
        [&] (const std::string& path) {
            ++fillCount;
            TOFStream output((TFsPath(path) / "data").GetPath());
            output << "payload";
            output.Finish();
            started.Set();
            return gate.ToFuture();
        });
    WaitFor(started.ToFuture()).ThrowOnError();
    auto second = storage->GetOrCreate(id, MakeFiller("other", &fillCount));
    first.Cancel(TError("Canceled by first caller"));
    gate.Set();

    auto firstObject = WaitFor(first).ValueOrThrow();
    auto secondObject = WaitFor(second).ValueOrThrow();
    EXPECT_EQ(fillCount, 1);
    EXPECT_EQ(ReadPayload(firstObject), "payload");
    EXPECT_EQ(ReadPayload(secondObject), "payload");
}

TEST(TFileStorageTest, RejectsConcurrentOwnerAndReusesLockAfterExit)
{
    TStorageFixture fixture;
    int fillCount = 0;
    auto id = TFileStorageObjectId("test:v1:locked-root");
    auto firstStorage = fixture.MakeStorage();
    auto first = WaitFor(firstStorage->GetOrCreate(
        id,
        MakeFiller("payload", &fillCount)))
        .ValueOrThrow();

    EXPECT_THROW_WITH_SUBSTRING(
        fixture.MakeStorage(),
        "already owned by another process");
    EXPECT_TRUE((TFsPath(fixture.Root.Name()) / ".lock").Exists());

    first.Reset();
    firstStorage.Reset();
    auto restartedStorage = fixture.MakeStorage();
    auto restarted = WaitFor(restartedStorage->GetOrCreate(
        id,
        MakeFiller("other", &fillCount)))
        .ValueOrThrow();
    EXPECT_EQ(fillCount, 1);
    EXPECT_EQ(ReadPayload(restarted), "payload");
}

TEST(TFileStorageTest, ObjectDoesNotKeepStorageAlive)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage();
    auto weakStorage = MakeWeak(storage.Get());
    int fillCount = 0;
    auto object = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:weak-owner"),
        MakeFiller("payload", &fillCount)))
        .ValueOrThrow();

    storage.Reset();

    EXPECT_FALSE(weakStorage.Lock());
    EXPECT_EQ(ReadPayload(object), "payload");
}

TEST(TFileStorageTest, RejectsCandidateWithoutEvictingExistingObject)
{
    TStorageFixture fixture;
    int firstFillCount = 0;
    auto storage = fixture.MakeStorage(1, 1);
    auto firstId = TFileStorageObjectId("test:v1:first");
    auto first = WaitFor(storage->GetOrCreate(firstId, MakeFiller("x", &firstFillCount))).ValueOrThrow();
    first.Reset();

    int rejectedFillCount = 0;
    auto rejected = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:large"),
        MakeFiller("xx", &rejectedFillCount)));
    EXPECT_FALSE(rejected.IsOK());
    EXPECT_TRUE(fixture.HasStatusError("/capacity"));

    auto preserved = WaitFor(storage->GetOrCreate(firstId, MakeFiller("other", &firstFillCount))).ValueOrThrow();
    EXPECT_EQ(firstFillCount, 1);
    EXPECT_EQ(ReadPayload(preserved), "x");
}

TEST(TFileStorageTest, RejectsKnownOversizeCandidateBeforeFill)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(1, 1);
    int fillCount = 0;

    auto result = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:known-oversize"),
        2,
        MakeFiller("xx", &fillCount)));

    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(fillCount, 0);
    EXPECT_TRUE(fixture.HasStatusError("/capacity"));
}

TEST(TFileStorageTest, ConcurrentKnownSizeReservationsRespectHardLimit)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(3, 3);
    auto started = NewPromise<void>();
    auto gate = NewPromise<void>();
    int firstFillCount = 0;
    int secondFillCount = 0;

    auto first = storage->GetOrCreate(
        TFileStorageObjectId("test:v1:reserved-first"),
        2,
        [&] (const std::string& path) {
            ++firstFillCount;
            TOFStream output((TFsPath(path) / "data").GetPath());
            output << "aa";
            output.Finish();
            started.Set();
            return gate.ToFuture();
        });
    WaitFor(started.ToFuture()).ThrowOnError();

    auto second = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:reserved-second"),
        2,
        MakeFiller("bb", &secondFillCount)));
    EXPECT_FALSE(second.IsOK());
    EXPECT_EQ(secondFillCount, 0);

    gate.Set();
    auto firstObject = WaitFor(first).ValueOrThrow();
    EXPECT_EQ(firstFillCount, 1);
    EXPECT_EQ(ReadPayload(firstObject), "aa");
}

TEST(TFileStorageTest, EvictsLeastRecentlyUsedUnpinnedObject)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(2, 3);
    int firstCount = 0;
    int secondCount = 0;
    int thirdCount = 0;

    auto firstId = TFileStorageObjectId("test:v1:first");
    auto first = WaitFor(storage->GetOrCreate(firstId, MakeFiller("1", &firstCount))).ValueOrThrow();
    first.Reset();
    auto second = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:second"),
        MakeFiller("2", &secondCount)))
        .ValueOrThrow();
    second.Reset();
    auto third = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:third"),
        MakeFiller("3", &thirdCount)))
        .ValueOrThrow();
    third.Reset();

    auto refilled = WaitFor(storage->GetOrCreate(firstId, MakeFiller("1", &firstCount))).ValueOrThrow();
    EXPECT_EQ(firstCount, 2);
    EXPECT_EQ(ReadPayload(refilled), "1");
}

TEST(TFileStorageTest, PublishesPermissionPreservingReadOnlyPayload)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage();
    auto object = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:modes"),
        [] (const std::string& path) {
            auto file = TFsPath(path) / "data";
            TOFStream output(file.GetPath());
            output << "payload";
            output.Finish();
            THROW_ERROR_EXCEPTION_UNLESS(
                Chmod(file.GetPath().c_str(), 0640) == 0,
                "Failed to set test file permissions")
                .With(TError::FromSystem());
            return MakeFuture<void>(TError());
        }))
        .ValueOrThrow();

    TFileStat stat(TFsPath(object->GetPath()) / "data", /*nofollow*/ true);
    EXPECT_EQ(stat.Mode & 0777, 0440);
}

TEST(TFileStorageTest, WritableCorruptionIsDeletedAndRefilled)
{
    TStorageFixture fixture;
    int fillCount = 0;
    auto storage = fixture.MakeStorage();
    auto id = TFileStorageObjectId("test:v1:corrupt");
    auto object = WaitFor(storage->GetOrCreate(id, MakeFiller("old", &fillCount))).ValueOrThrow();
    auto file = TFsPath(object->GetPath()) / "data";
    object.Reset();
    storage.Reset();

    TFileStat stat(file, /*nofollow*/ true);
    THROW_ERROR_EXCEPTION_UNLESS(
        Chmod(file.GetPath().c_str(), stat.Mode | S_IWUSR) == 0,
        "Failed to corrupt test file permissions")
        .With(TError::FromSystem());
    storage = fixture.MakeStorage();
    auto repaired = WaitFor(storage->GetOrCreate(id, MakeFiller("new", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 2);
    EXPECT_EQ(ReadPayload(repaired), "new");
}

TEST(TFileStorageTest, ProbeDoesNotQuarantinePinnedInvalidObject)
{
    TStorageFixture fixture;
    TStorageFixture seedFixture;
    auto id = TFileStorageObjectId("test:v1:probe-pinned-invalid");

    int seedFillCount = 0;
    auto seedStorage = seedFixture.MakeStorage();
    auto seedObject = WaitFor(seedStorage->GetOrCreate(
        id,
        MakeFiller("seed", &seedFillCount)))
        .ValueOrThrow();
    auto seedObjectDirectory = TFsPath(seedObject->GetPath()).Parent();
    auto prefix = std::string(seedObjectDirectory.Parent().Basename());
    auto digest = std::string(seedObjectDirectory.Basename());
    seedObject.Reset();
    seedStorage.Reset();

    auto storage = fixture.MakeStorage();
    auto started = NewPromise<void>();
    auto gate = NewPromise<void>();
    int fillCount = 0;
    auto pending = storage->GetOrCreate(
        id,
        [&] (const std::string& path) {
            ++fillCount;
            TOFStream output((TFsPath(path) / "data").GetPath());
            output << "staged";
            output.Finish();
            started.Set();
            return gate.ToFuture();
        });
    WaitFor(started.ToFuture()).ThrowOnError();

    auto targetPrefix = TFsPath(fixture.Root.Name()) / "objects" / prefix;
    targetPrefix.MkDirs();
    auto targetObjectDirectory = targetPrefix / digest;
    seedObjectDirectory.RenameTo(targetObjectDirectory);
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();

    auto pinned = WaitFor(storage->GetOrCreate(
        id,
        MakeFiller("unused", &fillCount)))
        .ValueOrThrow();
    auto data = TFsPath(pinned->GetPath()) / "data";
    TFileStat stat(data, /*nofollow*/ true);
    ChangePermissions(data, stat.Mode | S_IWUSR);
    {
        TOFStream output(data.GetPath());
        output << "corrupt";
    }

    gate.Set();
    auto result = WaitFor(pending);
    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(targetObjectDirectory.Exists());
    EXPECT_TRUE(TFsPath(pinned->GetPath()).Exists());
    EXPECT_EQ(fillCount, 1);
}

TEST(TFileStorageTest, ReconciliationSweepsStagingAndInvalidFinalObject)
{
    TStorageFixture fixture;
    int fillCount = 0;
    auto id = TFileStorageObjectId("test:v1:recovery");
    auto storage = fixture.MakeStorage();
    auto object = WaitFor(storage->GetOrCreate(id, MakeFiller("old", &fillCount))).ValueOrThrow();
    auto objectDirectory = TFsPath(object->GetPath()).Parent();
    object.Reset();
    storage.Reset();

    auto staleStaging = TFsPath(fixture.Root.Name()) / "staging" / "stale";
    staleStaging.MkDir();
    (staleStaging / "payload").MkDir();
    (objectDirectory / "manifest.yson").DeleteIfExists();

    storage = fixture.MakeStorage();
    EXPECT_FALSE(staleStaging.Exists());
    auto repaired = WaitFor(storage->GetOrCreate(id, MakeFiller("new", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 2);
    EXPECT_EQ(ReadPayload(repaired), "new");
}

TEST(TFileStorageTest, ReconciliationCompletesInterruptedTrashDeletion)
{
    TStorageFixture fixture;
    int fillCount = 0;
    auto id = TFileStorageObjectId("test:v1:trash-recovery");
    auto storage = fixture.MakeStorage();
    auto object = WaitFor(storage->GetOrCreate(id, MakeFiller("old", &fillCount))).ValueOrThrow();
    auto objectDirectory = TFsPath(object->GetPath()).Parent();
    object.Reset();
    storage.Reset();

    auto trashEntry = TFsPath(fixture.Root.Name()) / "trash" / "3-crash";
    objectDirectory.RenameTo(trashEntry);
    EXPECT_TRUE(trashEntry.Exists());

    storage = fixture.MakeStorage();
    EXPECT_FALSE(trashEntry.Exists());
    auto refilled = WaitFor(storage->GetOrCreate(id, MakeFiller("new", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 2);
    EXPECT_EQ(ReadPayload(refilled), "new");
}

TEST(TFileStorageTest, UnknownRootEntryBlocksPublicationUntilReconciled)
{
    TStorageFixture fixture;
    auto foreignEntry = TFsPath(fixture.Root.Name()) / "foreign";
    foreignEntry.Touch();
    auto storage = fixture.MakeStorage();
    int fillCount = 0;
    auto id = TFileStorageObjectId("test:v1:foreign-root-entry");

    auto blocked = WaitFor(storage->GetOrCreate(
        id,
        1,
        MakeFiller("x", &fillCount)));
    EXPECT_FALSE(blocked.IsOK());
    EXPECT_EQ(fillCount, 0);
    EXPECT_TRUE(fixture.HasStatusError("/startup"));
    EXPECT_TRUE(fixture.HasStatusError("/capacity"));

    foreignEntry.DeleteIfExists();
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();
    auto recovered = WaitFor(storage->GetOrCreate(
        id,
        1,
        MakeFiller("x", &fillCount)))
        .ValueOrThrow();
    EXPECT_EQ(fillCount, 1);
    EXPECT_EQ(ReadPayload(recovered), "x");
    EXPECT_FALSE(fixture.HasStatusError("/startup"));
}

TEST(TFileStorageTest, PinnedObjectIsNotEvictedAndPinnedOveruseAlertClearsOnRelease)
{
    TStorageFixture fixture;
    int pinnedFillCount = 0;
    int otherFillCount = 0;
    auto storage = fixture.MakeStorage(1, 3);
    auto pinnedId = TFileStorageObjectId("test:v1:pinned");
    auto pinned = WaitFor(storage->GetOrCreate(
        pinnedId,
        MakeFiller("aa", &pinnedFillCount)))
        .ValueOrThrow();

    WaitForPredicate([&] {
        return fixture.HasStatusError("/pinned_overuse");
    });
    auto other = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:other"),
        MakeFiller("b", &otherFillCount)))
        .ValueOrThrow();
    other.Reset();
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();

    auto pinnedAgain = WaitFor(storage->GetOrCreate(
        pinnedId,
        MakeFiller("changed", &pinnedFillCount)))
        .ValueOrThrow();
    EXPECT_EQ(pinnedFillCount, 1);
    EXPECT_EQ(ReadPayload(pinnedAgain), "aa");

    pinned.Reset();
    pinnedAgain.Reset();
    WaitForPredicate([&] {
        return !fixture.HasStatusError("/pinned_overuse");
    });
}

TEST(TFileStorageTest, TransientValidationFailureDoesNotDropPinnedObject)
{
    TStorageFixture fixture;
    int fillCount = 0;
    auto storage = fixture.MakeStorage(10, 20);
    auto id = TFileStorageObjectId("test:v1:pinned-validation");
    auto pinned = WaitFor(storage->GetOrCreate(id, MakeFiller("payload", &fillCount))).ValueOrThrow();
    auto objectDirectory = TFsPath(pinned->GetPath()).Parent();
    auto originalMode = TFileStat(objectDirectory, /*nofollow*/ true).Mode;

    ChangePermissions(objectDirectory, S_IXUSR);
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();
    EXPECT_TRUE(fixture.HasStatusError("/startup"));
    EXPECT_TRUE(objectDirectory.Exists());

    auto same = WaitFor(storage->GetOrCreate(id, MakeFiller("changed", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 1);
    EXPECT_EQ(ReadPayload(same), "payload");

    ChangePermissions(objectDirectory, originalMode);
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();
    EXPECT_FALSE(fixture.HasStatusError("/startup"));
}

TEST(TFileStorageTest, DiskFullAlertSurvivesReconciliationUntilSuccessfulPublication)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(10, 20);
    int fillCount = 0;

    auto fillError = TError("Injected filler ENOSPC").With(TError::FromSystem(ENOSPC));
    auto failedFill = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:fill-enospc"),
        [&] (const std::string&) {
            ++fillCount;
            return MakeFuture<void>(fillError);
        }));
    EXPECT_FALSE(failedFill.IsOK());
    EXPECT_TRUE(fixture.HasStatusError("/disk_full"));
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();
    EXPECT_TRUE(fixture.HasStatusError("/disk_full"));

    auto recoveredFill = WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:fill-enospc"),
        MakeFiller("ok", &fillCount)))
        .ValueOrThrow();
    EXPECT_EQ(fillCount, 2);
    EXPECT_EQ(ReadPayload(recoveredFill), "ok");
    EXPECT_FALSE(fixture.HasStatusError("/disk_full"));
}

TEST(TFileStorageTest, MutationFailureDoesNotWedgeObjectId)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(10, 20);
    int fillCount = 0;
    auto id = TFileStorageObjectId("test:v1:rename-retry");
    auto objectsDirectory = TFsPath(fixture.Root.Name()) / "objects";
    auto originalMode = TFileStat(objectsDirectory, /*nofollow*/ true).Mode;

    ChangePermissions(objectsDirectory, S_IRUSR | S_IXUSR);
    EXPECT_FALSE(WaitFor(storage->GetOrCreate(id, MakeFiller("payload", &fillCount))).IsOK());

    ChangePermissions(objectsDirectory, originalMode);
    auto recovered = WaitFor(storage->GetOrCreate(id, MakeFiller("payload", &fillCount))).ValueOrThrow();
    EXPECT_EQ(fillCount, 2);
    EXPECT_EQ(ReadPayload(recovered), "payload");
}

TEST(TFileStorageTest, EvictionFailureBeforeUnlinkPreservesVictimAndAbortsPublication)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(1, 1);
    int victimFillCount = 0;
    auto victimId = TFileStorageObjectId("test:v1:victim");
    auto victim = WaitFor(storage->GetOrCreate(victimId, MakeFiller("v", &victimFillCount))).ValueOrThrow();
    auto objectDirectory = TFsPath(victim->GetPath()).Parent();
    auto objectPrefix = objectDirectory.Parent();
    auto originalMode = TFileStat(objectPrefix, /*nofollow*/ true).Mode;
    victim.Reset();

    ChangePermissions(objectPrefix, S_IRUSR | S_IXUSR);
    int candidateFillCount = 0;
    EXPECT_FALSE(WaitFor(storage->GetOrCreate(
        TFileStorageObjectId("test:v1:candidate"),
        MakeFiller("c", &candidateFillCount)))
            .IsOK());

    ChangePermissions(objectPrefix, originalMode);
    auto preserved = WaitFor(storage->GetOrCreate(
        victimId,
        MakeFiller("changed", &victimFillCount)))
        .ValueOrThrow();
    EXPECT_EQ(victimFillCount, 1);
    EXPECT_EQ(ReadPayload(preserved), "v");
}

TEST(TFileStorageTest, CleanupContinuesPastFailedVictim)
{
    TStorageFixture fixture;
    auto storage = fixture.MakeStorage(1, 3);
    int firstFillCount = 0;
    int secondFillCount = 0;
    auto firstId = TFileStorageObjectId("test:v1:cleanup-first");
    auto secondId = TFileStorageObjectId("test:v1:cleanup-second");
    auto first = WaitFor(storage->GetOrCreate(firstId, MakeFiller("a", &firstFillCount))).ValueOrThrow();
    auto second = WaitFor(storage->GetOrCreate(secondId, MakeFiller("b", &secondFillCount))).ValueOrThrow();
    auto firstObjectDirectory = TFsPath(first->GetPath()).Parent();
    auto firstObjectPrefix = firstObjectDirectory.Parent();
    auto secondObjectPrefix = TFsPath(second->GetPath()).Parent().Parent();
    ASSERT_NE(firstObjectPrefix, secondObjectPrefix);
    auto originalMode = TFileStat(firstObjectPrefix, /*nofollow*/ true).Mode;
    first.Reset();
    second.Reset();

    ChangePermissions(firstObjectPrefix, S_IRUSR | S_IXUSR);
    WaitFor(ReconcileFileStorageForTesting(storage)).ThrowOnError();
    EXPECT_TRUE(fixture.HasStatusError("/startup"));

    ChangePermissions(firstObjectPrefix, originalMode);
    auto firstHit = WaitFor(storage->GetOrCreate(
        firstId,
        MakeFiller("changed", &firstFillCount)))
        .ValueOrThrow();
    EXPECT_EQ(firstFillCount, 1);
    auto secondRefill = WaitFor(storage->GetOrCreate(
        secondId,
        MakeFiller("b", &secondFillCount)))
        .ValueOrThrow();
    EXPECT_EQ(secondFillCount, 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NFileStorage
