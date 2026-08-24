#include "file_storage.h"

#include "private.h"

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <library/cpp/yt/misc/guid.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <util/folder/path.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/fstat.h>
#include <util/system/sysstat.h>

#include <list>
#include <memory>

namespace NYT::NFlow::NFileStorage {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

static constexpr int ManifestFormatVersion = 1;

DEFINE_ENUM(EManifestEntryType,
    ((File)      (0))
    ((Directory) (1))
);

DECLARE_REFCOUNTED_STRUCT(TManifestEntry);

struct TManifestEntry
    : public TYsonStruct
{
    std::string RelativePath;
    EManifestEntryType Type{};
    i64 Size{};

    REGISTER_YSON_STRUCT(TManifestEntry);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("relative_path", &TThis::RelativePath);
        registrar.Parameter("type", &TThis::Type);
        registrar.Parameter("size", &TThis::Size);
    }
};

DEFINE_REFCOUNTED_TYPE(TManifestEntry);

DECLARE_REFCOUNTED_STRUCT(TManifest);

struct TManifest
    : public TYsonStruct
{
    int FormatVersion{};
    std::string ObjectId;
    i64 TotalSize{};
    std::vector<TManifestEntryPtr> Entries;

    REGISTER_YSON_STRUCT(TManifest);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("format_version", &TThis::FormatVersion);
        registrar.Parameter("object_id", &TThis::ObjectId);
        registrar.Parameter("total_size", &TThis::TotalSize);
        registrar.Parameter("entries", &TThis::Entries);
    }
};

DEFINE_REFCOUNTED_TYPE(TManifest);

std::string GetDigest(TStringBuf id)
{
    NCrypto::TSha256Hasher hasher;
    hasher.Append(id);
    return hasher.GetHexDigestLowerCase();
}

bool IsSpecialBasename(TStringBuf basename)
{
    return basename.empty() || basename == "." || basename == "..";
}

TFileStat GetFileStatus(const TFsPath& path)
{
    return TFileStat(path, /*nofollow*/ true);
}

void ListDirectory(const TFsPath& path, TVector<TFsPath>* children)
{
    path.List(*children);
}

std::string ReadFile(const TFsPath& path)
{
    TFileInput input(path.GetPath());
    auto value = input.ReadAll();
    return std::string(value.data(), value.size());
}

void CreateDirectory(const TFsPath& path)
{
    path.MkDir();
}

void MakeWritable(const TFsPath& path)
{
    if (!path.Exists()) {
        return;
    }

    TFileStat stat(path, /*nofollow*/ true);
    if (stat.IsSymlink()) {
        return;
    }
    if (!stat.IsDir()) {
        THROW_ERROR_EXCEPTION_UNLESS(
            Chmod(path.GetPath().c_str(), stat.Mode | S_IWUSR) == 0,
            "Failed to make file storage entry writable: %Qv",
            path.GetPath())
            .With(TError::FromSystem());
        return;
    }

    THROW_ERROR_EXCEPTION_UNLESS(
        Chmod(path.GetPath().c_str(), stat.Mode | S_IRUSR | S_IWUSR | S_IXUSR) == 0,
        "Failed to make file storage directory writable: %Qv",
        path.GetPath())
        .With(TError::FromSystem());
    TVector<TFsPath> children;
    path.List(children);
    for (const auto& child : children) {
        MakeWritable(child);
    }
}

void RemoveTree(const TFsPath& path)
{
    MakeWritable(path);
    path.ForceDelete();
}

void Rename(const TFsPath& source, const TFsPath& destination)
{
    source.RenameTo(destination);
}

void WriteFile(const TFsPath& path, TStringBuf value)
{
    TOFStream output(path.GetPath());
    output.Write(value.data(), value.size());
    output.Finish();
}

void SetPermissions(const TFsPath& path, int mode)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Chmod(path.GetPath().c_str(), mode) == 0,
        "Failed to change permissions of %Qv",
        path.GetPath())
        .With(TError::FromSystem());
}

void FlushFile(const TFsPath& path)
{
    TFile(path.GetPath(), OpenExisting | RdOnly).FlushData();
}

void FlushDirectory(const TFsPath& path)
{
    NFS::FlushDirectory(path.GetPath());
}

void CollectEntries(
    const TFsPath& root,
    const TFsPath& current,
    std::vector<TManifestEntryPtr>* entries,
    i64* totalSize)
{
    TVector<TFsPath> children;
    ListDirectory(current, &children);
    Sort(children, [] (const TFsPath& lhs, const TFsPath& rhs) {
        return lhs.GetPath() < rhs.GetPath();
    });

    for (const auto& child : children) {
        auto stat = GetFileStatus(child);
        THROW_ERROR_EXCEPTION_IF(
            stat.IsSymlink(),
            "File storage payload contains symlink %Qv",
            child.GetPath());

        auto relativePath = std::string(child.GetPath()).substr(std::string(root.GetPath()).size() + 1);
        THROW_ERROR_EXCEPTION_IF(
            IsSpecialBasename(relativePath) || relativePath.starts_with("../"),
            "File storage payload entry escapes its root: %Qv",
            child.GetPath());

        auto entry = New<TManifestEntry>();
        entry->RelativePath = std::move(relativePath);
        if (stat.IsDir()) {
            entry->Type = EManifestEntryType::Directory;
            CollectEntries(root, child, entries, totalSize);
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(
                stat.IsFile(),
                "File storage payload contains unsupported entry %Qv",
                child.GetPath());
            entry->Type = EManifestEntryType::File;
            entry->Size = stat.Size;
            *totalSize += entry->Size;
        }
        entries->push_back(std::move(entry));
    }
}

TManifestPtr BuildManifest(
    const TFsPath& payload,
    const TFileStorageObjectId& id)
{
    auto manifest = New<TManifest>();
    manifest->FormatVersion = ManifestFormatVersion;
    manifest->ObjectId = id.Underlying();
    CollectEntries(payload, payload, &manifest->Entries, &manifest->TotalSize);
    Sort(manifest->Entries, [] (const auto& lhs, const auto& rhs) {
        return lhs->RelativePath < rhs->RelativePath;
    });
    return manifest;
}

void ValidateStagingRoot(const TFsPath& stagingDirectory)
{
    TVector<TFsPath> entries;
    ListDirectory(stagingDirectory, &entries);
    auto payloadStatus = entries.size() == 1
        ? std::optional(GetFileStatus(entries.front()))
        : std::nullopt;
    THROW_ERROR_EXCEPTION_UNLESS(
        entries.size() == 1 &&
            entries.front().Basename() == "payload" &&
            payloadStatus->IsDir() &&
            !payloadStatus->IsSymlink(),
        "File storage filler must create entries only inside the supplied payload directory")
        .With("staging_path", stagingDirectory.GetPath());
}

void MakeTreeReadOnly(const TFsPath& path)
{
    auto stat = GetFileStatus(path);
    THROW_ERROR_EXCEPTION_IF(
        stat.IsSymlink(),
        "File storage payload contains symlink %Qv",
        path.GetPath());

    if (stat.IsDir()) {
        TVector<TFsPath> children;
        ListDirectory(path, &children);
        for (const auto& child : children) {
            MakeTreeReadOnly(child);
        }
        SetPermissions(
            path,
            (stat.Mode & ~(S_IWUSR | S_IWGRP | S_IWOTH)) | S_IRUSR | S_IXUSR);
    } else {
        THROW_ERROR_EXCEPTION_UNLESS(
            stat.IsFile(),
            "File storage payload contains unsupported entry %Qv",
            path.GetPath());
        SetPermissions(
            path,
            stat.Mode & ~(S_IWUSR | S_IWGRP | S_IWOTH));
    }
}

void FlushTree(const TFsPath& path)
{
    auto stat = GetFileStatus(path);
    if (stat.IsDir()) {
        TVector<TFsPath> children;
        ListDirectory(path, &children);
        for (const auto& child : children) {
            FlushTree(child);
        }
        FlushDirectory(path);
    } else {
        FlushFile(path);
    }
}

bool IsTreeReadOnly(const TFsPath& path)
{
    auto stat = GetFileStatus(path);
    if (stat.IsSymlink() || (stat.Mode & (S_IWUSR | S_IWGRP | S_IWOTH)) != 0) {
        return false;
    }
    if (!stat.IsDir()) {
        return stat.IsFile();
    }

    TVector<TFsPath> children;
    ListDirectory(path, &children);
    for (const auto& child : children) {
        if (!IsTreeReadOnly(child)) {
            return false;
        }
    }
    return true;
}

bool AreManifestsEqual(const TManifestPtr& lhs, const TManifestPtr& rhs)
{
    if (lhs->TotalSize != rhs->TotalSize || lhs->Entries.size() != rhs->Entries.size()) {
        return false;
    }
    for (int index = 0; index < std::ssize(lhs->Entries); ++index) {
        const auto& lhsEntry = lhs->Entries[index];
        const auto& rhsEntry = rhs->Entries[index];
        if (lhsEntry->RelativePath != rhsEntry->RelativePath ||
            lhsEntry->Type != rhsEntry->Type ||
            lhsEntry->Size != rhsEntry->Size)
        {
            return false;
        }
    }
    return true;
}

std::string MakeTrashEntryName(std::optional<i64> size)
{
    return Format("%v-%v",
        size ? ToString(*size) : "unknown",
        TGuid::Create());
}

std::optional<i64> ParseTrashEntrySize(TStringBuf name)
{
    auto separator = name.find('-');
    if (separator == TStringBuf::npos) {
        return std::nullopt;
    }
    try {
        auto size = FromString<i64>(name.substr(0, separator));
        return size >= 0 ? std::optional(size) : std::nullopt;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

struct TObjectEntry
{
    TFileStorageObjectId Id;
    std::string Digest;
    std::string Path;
    i64 Size = 0;
    int PinCount = 0;
    bool Indexed = true;
    bool InLru = false;
    std::list<std::string>::iterator LruIterator;
};

using TObjectEntryPtr = std::shared_ptr<TObjectEntry>;

enum class EObjectValidationResult
{
    Valid,
    Invalid,
    Error,
};

enum class EEvictionResult
{
    Evicted,
    Skipped,
    Failed,
};

struct TInflightEntry
{
    TFuture<IFileStorageObjectPtr> Future;
    TGuid RequestId;
};

struct TMetricsSnapshot
{
    i64 Size = 0;
    i64 ObjectCount = 0;
    i64 PinnedSize = 0;
    i64 ReservedSize = 0;
    i64 TrashSize = 0;
};

class TFileStorage;

class TFileStorageObject
    : public IFileStorageObject
{
public:
    TFileStorageObject(
        TWeakPtr<TFileStorage> owner,
        TObjectEntryPtr entry);

    ~TFileStorageObject() override;

    const TFileStorageObjectId& GetId() const override
    {
        return Entry_->Id;
    }

    const std::string& GetPath() const override
    {
        return Entry_->Path;
    }

private:
    const TWeakPtr<TFileStorage> Owner_;
    const TObjectEntryPtr Entry_;
};

class TFileStorage
    : public IFileStorage
{
public:
    TFileStorage(
        TFileStorageConfigPtr config,
        IInvokerPtr invoker,
        TLogger logger,
        TProfiler profiler,
        IStatusProfilerPtr statusProfiler)
        : Config_(std::move(config))
        , Root_(Config_->Path)
        , Invoker_(CreateSerializedInvoker(std::move(invoker), "FileStorage"))
        , Logger_(std::move(logger))
        , CapacityError_(statusProfiler->ErrorState("/capacity"))
        , DiskFullError_(statusProfiler->ErrorState("/disk_full"))
        , PinnedOveruseError_(statusProfiler->ErrorState("/pinned_overuse"))
        , StartupError_(statusProfiler->ErrorState("/startup"))
        , SizeGauge_(profiler.Gauge("/size"))
        , ObjectCountGauge_(profiler.Gauge("/object_count"))
        , PinnedSizeGauge_(profiler.Gauge("/pinned_size"))
        , ReservedSizeGauge_(profiler.Gauge("/reserved_size"))
        , TrashSizeGauge_(profiler.Gauge("/trash_size"))
        , HitCounter_(profiler.Counter("/hit_count"))
        , MissCounter_(profiler.Counter("/miss_count"))
        , FillFailureCounter_(profiler.Counter("/fill_failure_count"))
        , EvictionCounter_(profiler.Counter("/eviction_count"))
        , EvictedBytesCounter_(profiler.Counter("/evicted_bytes"))
        , RejectedPublicationCounter_(profiler.Counter("/rejected_publication_count"))
        , CleanupExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TFileStorage::Reconcile, MakeWeak(this)),
            Config_->CleanupPeriod))
    {
        THROW_ERROR_EXCEPTION_UNLESS(
            Root_.IsAbsolute(),
            "File storage path %Qv must be absolute",
            Root_.GetPath());
        THROW_ERROR_EXCEPTION_UNLESS(
            Config_->SoftSizeLimit > 0 &&
                Config_->HardSizeLimit >= Config_->SoftSizeLimit,
            "Invalid file storage size limits");

        EnsureDirectoryDurable(Root_);
        RootLock_ = std::make_unique<TFileLock>((Root_ / ".lock").GetPath());
        THROW_ERROR_EXCEPTION_UNLESS(
            RootLock_->TryAcquire(),
            "File storage root %Qv is already owned by another process",
            Root_.GetPath());
        FlushDirectory(Root_);

        Reconcile();
        CleanupExecutor_->Start();
    }

    TFuture<IFileStorageObjectPtr> GetOrCreate(
        TFileStorageObjectId id,
        TFileStorageFiller filler) override
    {
        return GetOrCreate(
            std::move(id),
            std::nullopt,
            std::move(filler));
    }

    TFuture<IFileStorageObjectPtr> GetOrCreate(
        TFileStorageObjectId id,
        std::optional<i64> expectedSize,
        TFileStorageFiller filler) override
    {
        THROW_ERROR_EXCEPTION_IF(
            id.Underlying().empty(),
            "File storage object id must be nonempty");
        THROW_ERROR_EXCEPTION_IF(
            expectedSize && *expectedSize < 0,
            "File storage expected size must be nonnegative");

        IFileStorageObjectPtr hit;
        {
            auto guard = Guard(Lock_);
            if (auto it = Objects_.find(id.Underlying()); it != Objects_.end()) {
                hit = PinLocked(it->second);
            } else if (auto it = Inflight_.find(id.Underlying()); it != Inflight_.end()) {
                if (!it->second.Future.IsSet()) {
                    return it->second.Future;
                }
                Inflight_.erase(it);
            }
        }
        if (hit) {
            HitCounter_.Increment();
            RefreshMetrics();
            return MakeFuture<IFileStorageObjectPtr>(std::move(hit));
        }

        auto promise = NewPromise<IFileStorageObjectPtr>();
        auto future = promise.ToFuture().ToUncancelable();
        auto requestId = TGuid::Create();
        {
            auto guard = Guard(Lock_);
            if (auto it = Objects_.find(id.Underlying()); it != Objects_.end()) {
                hit = PinLocked(it->second);
            } else if (auto it = Inflight_.find(id.Underlying()); it != Inflight_.end()) {
                if (!it->second.Future.IsSet()) {
                    return it->second.Future;
                }
                Inflight_.erase(it);
            } else {
                Inflight_.emplace(
                    id.Underlying(),
                    TInflightEntry{
                        .Future = future,
                        .RequestId = requestId,
                    });
            }
        }
        if (hit) {
            HitCounter_.Increment();
            RefreshMetrics();
            return MakeFuture<IFileStorageObjectPtr>(std::move(hit));
        }

        auto result = BIND(
            &TFileStorage::DoGetOrCreate,
            MakeStrong(this),
            id,
            expectedSize,
            std::move(filler))
            .AsyncVia(Invoker_)
            .Run();
        result.Subscribe(BIND([
            weakThis = MakeWeak(this),
            promise,
            rawId = id.Underlying(),
            requestId
        ] (const TErrorOr<IFileStorageObjectPtr>& result) {
            if (auto strongThis = weakThis.Lock()) {
                auto guard = Guard(strongThis->Lock_);
                auto it = strongThis->Inflight_.find(rawId);
                if (it != strongThis->Inflight_.end() &&
                    it->second.RequestId == requestId)
                {
                    strongThis->Inflight_.erase(it);
                }
            }
            promise.Set(result);
        }).Via(Invoker_));
        return future;
    }

    TFuture<void> ReconcileForTesting()
    {
        return BIND(&TFileStorage::Reconcile, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    void Release(const TObjectEntryPtr& entry)
    {
        {
            auto guard = Guard(Lock_);
            if (entry->PinCount > 0) {
                --entry->PinCount;
            }
            if (entry->Indexed && entry->PinCount == 0 && !entry->InLru) {
                Lru_.push_back(entry->Id.Underlying());
                entry->LruIterator = std::prev(Lru_.end());
                entry->InLru = true;
            }
        }
        RefreshMetrics();
    }

private:
    const TLogger& Logger() const
    {
        return Logger_;
    }

    IFileStorageObjectPtr DoGetOrCreate(
        TFileStorageObjectId id,
        std::optional<i64> expectedSize,
        TFileStorageFiller filler)
    {
        const auto digest = GetDigest(id.Underlying());
        const auto finalDirectory = GetObjectDirectory(digest);

        if (auto object = Probe(id, digest, finalDirectory)) {
            HitCounter_.Increment();
            return object;
        }
        MissCounter_.Increment();

        auto stagingDirectory = Root_ / "staging" / ToString(TGuid::Create());
        auto payload = stagingDirectory / "payload";
        i64 reservedSize = 0;
        bool reservationActive = false;
        auto reservationGuard = Finally([&] {
            if (reservationActive) {
                ReleaseReservation(reservedSize);
            }
        });
        std::optional<TAsyncSemaphoreGuard> unknownFillGuard;

        try {
            if (expectedSize) {
                ReserveCapacity(*expectedSize);
                reservedSize = *expectedSize;
                reservationActive = true;
            } else {
                unknownFillGuard.emplace(
                    WaitFor(UnknownFillSemaphore_->AsyncAcquire(1).AsUnique())
                        .ValueOrThrow());
            }

            EnsureLayout();
            CreateDirectory(stagingDirectory);
            CreateDirectory(payload);
            FlushDirectory(Root_ / "staging");
            {
                auto guard = Guard(Lock_);
                ActiveStaging_.insert(stagingDirectory.GetPath());
            }

            WaitFor(filler(payload.GetPath())).ThrowOnError();
            ValidateStagingRoot(stagingDirectory);
            auto manifest = BuildManifest(payload, id);
            MakeTreeReadOnly(payload);
            FlushTree(payload);

            auto manifestYson = ConvertToYsonString(manifest);
            auto manifestPath = stagingDirectory / "manifest.yson";
            WriteFile(manifestPath, manifestYson.AsStringBuf());
            FlushFile(manifestPath);
            FlushDirectory(stagingDirectory);

            if (auto object = Probe(id, digest, finalDirectory)) {
                RemoveTree(stagingDirectory);
                FlushDirectory(Root_ / "staging");
                {
                    auto guard = Guard(Lock_);
                    ActiveStaging_.erase(stagingDirectory.GetPath());
                }
                return object;
            }

            if (reservationActive) {
                ReleaseReservation(reservedSize);
                reservationActive = false;
            }
            EnsurePublicationCapacity(manifest->TotalSize);
            EnsureLayout();
            auto prefixDirectory = Root_ / "objects" / digest.substr(0, 2);
            EnsureDirectoryDurable(prefixDirectory);
            FlushDirectory(Root_ / "objects");
            THROW_ERROR_EXCEPTION_IF(
                finalDirectory.Exists(),
                "File storage object directory appeared during publication")
                .With("object_id", id.Underlying())
                .With("path", finalDirectory.GetPath());
            Rename(stagingDirectory, finalDirectory);
            FlushDirectory(prefixDirectory);

            {
                auto guard = Guard(Lock_);
                ActiveStaging_.erase(stagingDirectory.GetPath());
            }
            auto object = AdoptAndPin(id, digest, finalDirectory, manifest);
            CapacityError_->ClearError();
            DiskFullError_->ClearError();
            return object;
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            FillFailureCounter_.Increment();
            if (NFS::IsOutOfDiskSpaceError(error)) {
                DiskFullError_->SetError(
                    TError("File storage ran out of disk space").With(error));
                YT_UNUSED_FUTURE(
                    BIND(&TFileStorage::Reconcile, MakeWeak(this))
                        .AsyncVia(Invoker_)
                        .Run());
            }
            {
                auto guard = Guard(Lock_);
                ActiveStaging_.erase(stagingDirectory.GetPath());
            }
            try {
                if (stagingDirectory.Exists()) {
                    RemoveTree(stagingDirectory);
                    if ((Root_ / "staging").Exists()) {
                        FlushDirectory(Root_ / "staging");
                    }
                }
            } catch (const std::exception& cleanupEx) {
                StartupError_->SetError(
                    TError("Failed to clean file storage staging directory")
                        .With(cleanupEx));
            }
            throw;
        }
    }

    IFileStorageObjectPtr Probe(
        const TFileStorageObjectId& id,
        const std::string& digest,
        const TFsPath& finalDirectory)
    {
        if (!finalDirectory.Exists()) {
            return nullptr;
        }

        TManifestPtr manifest;
        bool collision = false;
        TError validationError;
        auto validationResult = ValidateObject(
            finalDirectory,
            id,
            &manifest,
            &collision,
            &validationError);
        if (validationResult == EObjectValidationResult::Valid) {
            return AdoptAndPin(id, digest, finalDirectory, manifest);
        }
        if (validationResult == EObjectValidationResult::Error) {
            THROW_ERROR validationError;
        }
        if (collision) {
            THROW_ERROR_EXCEPTION(
                "File storage digest collision for object id %Qv",
                id.Underlying())
                .With("digest", digest);
        }

        std::optional<std::string> indexedId;
        {
            auto guard = Guard(Lock_);
            for (const auto& [rawId, entry] : Objects_) {
                if (entry->Digest == digest &&
                    entry->Path == (finalDirectory / "payload").GetPath())
                {
                    indexedId = rawId;
                    break;
                }
            }
        }

        TError quarantineError;
        if (indexedId) {
            // Eviction is skipped without an error of its own when the object is merely pinned.
            if (Evict(*indexedId, &quarantineError) != EEvictionResult::Evicted) {
                THROW_ERROR_EXCEPTION("Invalid indexed file storage object is pinned or changed during quarantine")
                    .With("object_id", *indexedId)
                    .With("path", finalDirectory.GetPath())
                    .WithIf(!quarantineError.IsOK(), std::move(quarantineError));
            }
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(
                QuarantineUnknownPath(finalDirectory, &quarantineError),
                "Failed to quarantine invalid file storage object")
                .With("object_id", id.Underlying())
                .With(quarantineError);
        }
        if (finalDirectory.Exists()) {
            THROW_ERROR_EXCEPTION(
                "Invalid file storage object remains at its final path")
                .With("object_id", id.Underlying())
                .With("path", finalDirectory.GetPath());
        }
        return nullptr;
    }

    EObjectValidationResult ValidateObject(
        const TFsPath& finalDirectory,
        const std::optional<TFileStorageObjectId>& expectedId,
        TManifestPtr* manifest,
        bool* collision,
        TError* error) const
    {
        *collision = false;

        std::string manifestYson;
        try {
            auto directoryStat = GetFileStatus(finalDirectory);
            if (!directoryStat.IsDir() || directoryStat.IsSymlink()) {
                return EObjectValidationResult::Invalid;
            }

            TVector<TFsPath> rootEntries;
            ListDirectory(finalDirectory, &rootEntries);
            if (rootEntries.size() != 2) {
                return EObjectValidationResult::Invalid;
            }
            THashSet<std::string> names;
            for (const auto& entry : rootEntries) {
                auto stat = GetFileStatus(entry);
                if (stat.IsSymlink()) {
                    return EObjectValidationResult::Invalid;
                }
                names.insert(std::string(entry.Basename()));
            }
            if (names != THashSet<std::string>{"manifest.yson", "payload"}) {
                return EObjectValidationResult::Invalid;
            }

            auto manifestPath = finalDirectory / "manifest.yson";
            auto payload = finalDirectory / "payload";
            if (!manifestPath.Exists() || !payload.Exists()) {
                return EObjectValidationResult::Invalid;
            }
            auto manifestStatus = GetFileStatus(manifestPath);
            auto payloadStatus = GetFileStatus(payload);
            if (!manifestStatus.IsFile() || !payloadStatus.IsDir() ||
                !IsTreeReadOnly(payload))
            {
                return EObjectValidationResult::Invalid;
            }
            manifestYson = ReadFile(manifestPath);
        } catch (const std::exception& ex) {
            *error = TError("Failed to read file storage object during validation")
                .With("path", finalDirectory.GetPath())
                .With(ex);
            return EObjectValidationResult::Error;
        }

        TManifestPtr parsed;
        try {
            parsed = ConvertTo<TManifestPtr>(TYsonString(manifestYson));
        } catch (const std::exception&) {
            return EObjectValidationResult::Invalid;
        }
        if (parsed->FormatVersion != ManifestFormatVersion) {
            return EObjectValidationResult::Invalid;
        }
        auto digest = GetDigest(parsed->ObjectId);
        if (digest != finalDirectory.Basename()) {
            return EObjectValidationResult::Invalid;
        }
        if (expectedId && parsed->ObjectId != expectedId->Underlying()) {
            *collision = GetDigest(expectedId->Underlying()) == digest;
            return EObjectValidationResult::Invalid;
        }

        TManifestPtr actual;
        try {
            actual = BuildManifest(
                finalDirectory / "payload",
                TFileStorageObjectId(parsed->ObjectId));
        } catch (const std::exception& ex) {
            *error = TError("Failed to inspect file storage payload during validation")
                .With("path", finalDirectory.GetPath())
                .With(ex);
            return EObjectValidationResult::Error;
        }
        if (!AreManifestsEqual(actual, parsed)) {
            return EObjectValidationResult::Invalid;
        }
        *manifest = std::move(parsed);
        return EObjectValidationResult::Valid;
    }

    IFileStorageObjectPtr AdoptAndPin(
        const TFileStorageObjectId& id,
        const std::string& digest,
        const TFsPath& directory,
        const TManifestPtr& manifest)
    {
        IFileStorageObjectPtr result;
        {
            auto guard = Guard(Lock_);
            auto it = Objects_.find(id.Underlying());
            if (it == Objects_.end()) {
                auto entry = std::make_shared<TObjectEntry>();
                entry->Id = id;
                entry->Digest = digest;
                entry->Path = (directory / "payload").GetPath();
                entry->Size = manifest->TotalSize;
                entry->InLru = true;
                Lru_.push_back(id.Underlying());
                entry->LruIterator = std::prev(Lru_.end());
                it = Objects_.emplace(id.Underlying(), std::move(entry)).first;
            }
            result = PinLocked(it->second);
        }
        RefreshMetrics();
        return result;
    }

    IFileStorageObjectPtr PinLocked(const TObjectEntryPtr& entry)
    {
        if (entry->PinCount == 0 && entry->InLru) {
            Lru_.erase(entry->LruIterator);
            entry->InLru = false;
        }
        ++entry->PinCount;
        return New<TFileStorageObject>(MakeWeak(this), entry);
    }

    void EnsureLayout()
    {
        EnsureDirectoryDurable(Root_);
        EnsureDirectoryDurable(Root_ / "staging");
        EnsureDirectoryDurable(Root_ / "objects");
        EnsureDirectoryDurable(Root_ / "trash");
    }

    void EnsureDirectoryDurable(const TFsPath& path)
    {
        if (path.Exists()) {
            THROW_ERROR_EXCEPTION_UNLESS(
                path.IsDirectory() && !path.IsSymlink(),
                "File storage layout path %Qv is not a directory",
                path.GetPath());
            return;
        }

        auto parent = path.Parent();
        THROW_ERROR_EXCEPTION_UNLESS(
            parent != path,
            "Cannot create file storage directory %Qv",
            path.GetPath());
        EnsureDirectoryDurable(parent);
        CreateDirectory(path);
        FlushDirectory(parent);
    }

    TFsPath GetObjectDirectory(const std::string& digest) const
    {
        return Root_ / "objects" / digest.substr(0, 2) / digest;
    }

    void EnsurePublicationCapacity(i64 candidateSize)
    {
        THashSet<std::string> excluded;
        std::optional<TError> evictionError;
        bool initialCheck = true;

        while (true) {
            std::vector<std::string> plan;
            i64 totalSize = 0;
            i64 pinnedSize = 0;
            std::optional<TError> rejection;
            std::optional<TError> rejectionAlert;
            {
                auto guard = Guard(Lock_);
                std::tie(totalSize, pinnedSize) = GetSizesLocked();
                totalSize += ReservedSize_;
                if (!UnknownRootEntries_.empty() || !UnknownTrashEntries_.empty()) {
                    auto error = TError("File storage cannot publish while cache bytes are unaccounted")
                        .With("unknown_root_entries", UnknownRootEntries_)
                        .With("unknown_trash_entries", UnknownTrashEntries_);
                    rejection = error;
                    rejectionAlert = std::move(error);
                } else if (initialCheck &&
                    pinnedSize + ReservedSize_ + candidateSize > Config_->HardSizeLimit)
                {
                    auto error = TError("File storage hard size limit exceeded")
                        .With("pinned_size", pinnedSize)
                        .With("reserved_size", ReservedSize_)
                        .With("candidate_size", candidateSize)
                        .With("hard_size_limit", Config_->HardSizeLimit);
                    rejection = error;
                    rejectionAlert = std::move(error);
                } else {
                    initialCheck = false;

                    i64 plannedBytes = 0;
                    for (const auto& rawId : Lru_) {
                        if (excluded.contains(rawId)) {
                            continue;
                        }
                        const auto& entry = GetOrCrash(Objects_, rawId);
                        plan.push_back(rawId);
                        plannedBytes += entry->Size;
                        if (totalSize - plannedBytes + candidateSize <= Config_->SoftSizeLimit) {
                            break;
                        }
                    }

                    if (totalSize + candidateSize <= Config_->SoftSizeLimit) {
                        plan.clear();
                    } else if (
                        totalSize + candidateSize > Config_->HardSizeLimit &&
                        totalSize - plannedBytes + candidateSize > Config_->HardSizeLimit)
                    {
                        auto error = evictionError.value_or(
                            TError("File storage could not reclaim enough capacity"));
                        rejectionAlert = TError("File storage hard size limit remains exceeded")
                            .With("current_size", totalSize)
                            .With("candidate_size", candidateSize)
                            .With("hard_size_limit", Config_->HardSizeLimit)
                            .With(error);
                        rejection = std::move(error);
                    }
                }
            }

            if (rejection) {
                CapacityError_->SetError(*rejectionAlert);
                RejectedPublicationCounter_.Increment();
                THROW_ERROR* rejection;
            }

            if (plan.empty()) {
                CapacityError_->ClearError();
                return;
            }

            bool mustReplan = false;
            for (const auto& rawId : plan) {
                TError error;
                auto result = Evict(rawId, &error);
                if (result == EEvictionResult::Failed) {
                    excluded.insert(rawId);
                    evictionError = error;
                    mustReplan = true;
                    break;
                }
                if (result == EEvictionResult::Skipped) {
                    mustReplan = true;
                    break;
                }
            }
            if (!mustReplan) {
                CapacityError_->ClearError();
                return;
            }
        }
    }

    void ReserveCapacity(i64 size)
    {
        EnsurePublicationCapacity(size);

        {
            auto guard = Guard(Lock_);
            ReservedSize_ += size;
        }
        RefreshMetrics();
    }

    void ReleaseReservation(i64 size)
    {
        {
            auto guard = Guard(Lock_);
            YT_VERIFY(ReservedSize_ >= size);
            ReservedSize_ -= size;
        }
        RefreshMetrics();
    }

    void EraseKnownTrashLocked(TStringBuf name)
    {
        auto it = TrashEntries_.find(name);
        if (it == TrashEntries_.end()) {
            return;
        }
        YT_VERIFY(TrashSize_ >= it->second);
        TrashSize_ -= it->second;
        TrashEntries_.erase(it);
    }

    EEvictionResult DeleteKnownTrashEntry(
        const std::string& name,
        const TFsPath& path,
        i64 size,
        TError* error)
    {
        try {
            if (path.Exists()) {
                RemoveTree(path);
            }
            FlushDirectory(Root_ / "trash");
            {
                auto guard = Guard(Lock_);
                EraseKnownTrashLocked(name);
            }
            RefreshMetrics();
            EvictionCounter_.Increment();
            EvictedBytesCounter_.Increment(size);
            return EEvictionResult::Evicted;
        } catch (const std::exception& ex) {
            *error = TError("Failed to remove file storage trash entry")
                .With("path", path.GetPath())
                .With("size", size)
                .With(ex);
            return EEvictionResult::Failed;
        }
    }

    bool DeleteUnknownTrashEntry(
        const std::string& name,
        const TFsPath& path,
        TError* error)
    {
        try {
            if (path.Exists()) {
                RemoveTree(path);
            }
            FlushDirectory(Root_ / "trash");
            {
                auto guard = Guard(Lock_);
                UnknownTrashEntries_.erase(name);
            }
            RefreshMetrics();
            return true;
        } catch (const std::exception& ex) {
            *error = TError("Failed to remove unaccounted file storage trash entry")
                .With("path", path.GetPath())
                .With(ex);
            return false;
        }
    }

    bool QuarantineUnknownPath(const TFsPath& source, TError* error)
    {
        auto name = MakeTrashEntryName(std::nullopt);
        auto destination = Root_ / "trash" / name;
        {
            auto guard = Guard(Lock_);
            UnknownTrashEntries_.insert(name);
        }
        RefreshMetrics();

        try {
            Rename(source, destination);
            if (source.Parent().Exists()) {
                FlushDirectory(source.Parent());
            }
            FlushDirectory(Root_ / "trash");
        } catch (const std::exception& ex) {
            if (source.Exists()) {
                {
                    auto guard = Guard(Lock_);
                    UnknownTrashEntries_.erase(name);
                }
                RefreshMetrics();
            }
            *error = TError("Failed to quarantine invalid file storage entry")
                .With("source_path", source.GetPath())
                .With("trash_path", destination.GetPath())
                .With(ex);
            return false;
        }

        return DeleteUnknownTrashEntry(name, destination, error);
    }

    EEvictionResult Evict(const std::string& rawId, TError* error)
    {
        TObjectEntryPtr entry;
        auto trashName = MakeTrashEntryName(std::nullopt);
        {
            auto guard = Guard(Lock_);
            auto it = Objects_.find(rawId);
            if (it == Objects_.end() || it->second->PinCount != 0) {
                return EEvictionResult::Skipped;
            }
            entry = it->second;
            trashName = MakeTrashEntryName(entry->Size);
            if (entry->InLru) {
                Lru_.erase(entry->LruIterator);
                entry->InLru = false;
            }
            entry->Indexed = false;
            Objects_.erase(it);
            EmplaceOrCrash(TrashEntries_, trashName, entry->Size);
            TrashSize_ += entry->Size;
        }
        RefreshMetrics();

        auto directory = GetObjectDirectory(entry->Digest);
        auto trashPath = Root_ / "trash" / trashName;
        try {
            Rename(directory, trashPath);
            if (directory.Parent().Exists()) {
                FlushDirectory(directory.Parent());
            }
            FlushDirectory(Root_ / "trash");
        } catch (const std::exception& ex) {
            *error = TError("Failed to move file storage object to trash")
                .With("object_id", rawId)
                .With("source_path", directory.GetPath())
                .With("trash_path", trashPath.GetPath())
                .With(ex);
            if (directory.Exists()) {
                {
                    auto guard = Guard(Lock_);
                    EraseKnownTrashLocked(trashName);
                    entry->Indexed = true;
                    entry->InLru = true;
                    Lru_.push_front(rawId);
                    entry->LruIterator = Lru_.begin();
                    EmplaceOrCrash(Objects_, rawId, entry);
                }
                RefreshMetrics();
            }
            return EEvictionResult::Failed;
        }

        return DeleteKnownTrashEntry(
            trashName,
            trashPath,
            entry->Size,
            error);
    }

    void Reconcile()
    {
        bool complete = true;
        try {
            EnsureLayout();
            ReconcileRootEntries(&complete);
            ReconcileStaging(&complete);
            ReconcileTrash(&complete);
            ReconcileObjects(&complete);
            CleanupToSoftLimit(&complete);
        } catch (const std::exception& ex) {
            complete = false;
            YT_TLOG_WARNING("File storage reconciliation failed")
                .With(ex);
        }

        if (complete) {
            StartupError_->ClearError();
        } else {
            THashSet<std::string> unknownRootEntries;
            THashSet<std::string> unknownTrashEntries;
            {
                auto guard = Guard(Lock_);
                unknownRootEntries = UnknownRootEntries_;
                unknownTrashEntries = UnknownTrashEntries_;
            }
            StartupError_->SetError(
                TError("File storage reconciliation did not cover the whole cache root")
                    .With("unknown_root_entries", unknownRootEntries)
                    .With("unknown_trash_entries", unknownTrashEntries));
        }
        RefreshMetrics();
    }

    void ReconcileRootEntries(bool* complete)
    {
        static const THashSet<std::string> KnownEntries = {
            ".lock",
            "staging",
            "objects",
            "trash",
        };

        THashSet<std::string> unknownEntries;
        TVector<TFsPath> entries;
        Root_.List(entries);
        for (const auto& entry : entries) {
            auto name = std::string(entry.Basename());
            if (!KnownEntries.contains(name)) {
                unknownEntries.insert(name);
            }
        }
        {
            auto guard = Guard(Lock_);
            UnknownRootEntries_ = unknownEntries;
        }
        if (!unknownEntries.empty()) {
            *complete = false;
            YT_TLOG_WARNING("File storage root contains unknown entries; publication is blocked")
                .With("Root", Root_.GetPath())
                .With("Entries", unknownEntries);
        }
    }

    void ReconcileStaging(bool* complete)
    {
        TVector<TFsPath> entries;
        (Root_ / "staging").List(entries);
        for (const auto& entry : entries) {
            {
                auto guard = Guard(Lock_);
                if (ActiveStaging_.contains(entry.GetPath())) {
                    continue;
                }
            }
            try {
                RemoveTree(entry);
            } catch (const std::exception& ex) {
                *complete = false;
                YT_TLOG_WARNING("Failed to sweep file storage staging entry")
                    .With("Path", entry.GetPath())
                    .With(ex);
            }
        }
        FlushDirectory(Root_ / "staging");
    }

    void ReconcileTrash(bool* complete)
    {
        THashSet<std::string> seenNames;
        TVector<TFsPath> entries;
        (Root_ / "trash").List(entries);
        for (const auto& entry : entries) {
            auto name = std::string(entry.Basename());
            seenNames.insert(name);
            auto size = ParseTrashEntrySize(name);
            {
                auto guard = Guard(Lock_);
                if (size) {
                    auto it = TrashEntries_.find(name);
                    if (it == TrashEntries_.end()) {
                        TrashEntries_.emplace(name, *size);
                        TrashSize_ += *size;
                    } else if (it->second != *size) {
                        YT_VERIFY(TrashSize_ >= it->second);
                        TrashSize_ -= it->second;
                        it->second = *size;
                        TrashSize_ += *size;
                    }
                } else {
                    UnknownTrashEntries_.insert(name);
                }
            }
            RefreshMetrics();

            TError error;
            bool removed = false;
            if (size) {
                removed = DeleteKnownTrashEntry(name, entry, *size, &error) ==
                    EEvictionResult::Evicted;
            } else {
                removed = DeleteUnknownTrashEntry(name, entry, &error);
            }
            if (!removed) {
                *complete = false;
                YT_TLOG_WARNING("Failed to sweep file storage trash entry")
                    .With("Path", entry.GetPath())
                    .With(error);
            }
        }

        {
            auto guard = Guard(Lock_);
            for (auto it = TrashEntries_.begin(); it != TrashEntries_.end();) {
                if (seenNames.contains(it->first)) {
                    ++it;
                } else {
                    YT_VERIFY(TrashSize_ >= it->second);
                    TrashSize_ -= it->second;
                    auto toErase = it++;
                    TrashEntries_.erase(toErase);
                }
            }
            for (auto it = UnknownTrashEntries_.begin(); it != UnknownTrashEntries_.end();) {
                if (seenNames.contains(*it)) {
                    ++it;
                } else {
                    auto toErase = it++;
                    UnknownTrashEntries_.erase(toErase);
                }
            }
        }
        FlushDirectory(Root_ / "trash");
        RefreshMetrics();
    }

    void ReconcileObjects(bool* complete)
    {
        struct TAdoption
        {
            TFileStorageObjectId Id;
            std::string Digest;
            std::string Path;
            i64 Size = 0;
            time_t MTime = 0;
            long MTimeNanoseconds = 0;
        };

        bool objectsComplete = true;
        std::vector<TAdoption> adoptions;
        THashSet<std::string> seenIds;
        TVector<TFsPath> prefixes;
        (Root_ / "objects").List(prefixes);
        for (const auto& prefix : prefixes) {
            try {
                TFileStat prefixStat(prefix, /*nofollow*/ true);
                if (!prefixStat.IsDir() || prefixStat.IsSymlink() || prefix.Basename().size() != 2) {
                    bool hasPinnedEntry = false;
                    {
                        auto guard = Guard(Lock_);
                        for (const auto& [rawId, entry] : Objects_) {
                            Y_UNUSED(rawId);
                            if (TFsPath(entry->Path).IsSubpathOf(prefix.GetPath()) &&
                                entry->PinCount > 0)
                            {
                                hasPinnedEntry = true;
                                break;
                            }
                        }
                        if (!hasPinnedEntry) {
                            for (auto it = Objects_.begin(); it != Objects_.end();) {
                                auto entry = it->second;
                                if (!TFsPath(entry->Path).IsSubpathOf(prefix.GetPath())) {
                                    ++it;
                                    continue;
                                }
                                if (entry->InLru) {
                                    Lru_.erase(entry->LruIterator);
                                    entry->InLru = false;
                                }
                                entry->Indexed = false;
                                auto toErase = it++;
                                Objects_.erase(toErase);
                            }
                        }
                    }
                    if (hasPinnedEntry) {
                        objectsComplete = false;
                        YT_TLOG_WARNING("Pinned file storage object is under an invalid prefix")
                            .With("Path", prefix.GetPath());
                        continue;
                    }
                    TError error;
                    if (!QuarantineUnknownPath(prefix, &error)) {
                        objectsComplete = false;
                        YT_TLOG_WARNING("Failed to quarantine invalid file storage prefix")
                            .With("Path", prefix.GetPath())
                            .With(error);
                    }
                    continue;
                }
                TVector<TFsPath> objects;
                prefix.List(objects);
                for (const auto& object : objects) {
                    try {
                        TManifestPtr manifest;
                        bool collision = false;
                        TError validationError;
                        auto validationResult = ValidateObject(
                            object,
                            std::nullopt,
                            &manifest,
                            &collision,
                            &validationError);
                        if (validationResult == EObjectValidationResult::Error) {
                            objectsComplete = false;
                            YT_TLOG_WARNING("Failed to validate file storage object during reconciliation")
                                .With("Path", object.GetPath())
                                .With(validationError);
                            continue;
                        }
                        if (validationResult == EObjectValidationResult::Invalid) {
                            std::optional<std::string> indexedId;
                            bool pinned = false;
                            {
                                auto guard = Guard(Lock_);
                                for (const auto& [rawId, entry] : Objects_) {
                                    if (entry->Digest == object.Basename() &&
                                        entry->Path == (object / "payload").GetPath())
                                    {
                                        indexedId = rawId;
                                        pinned = entry->PinCount > 0;
                                        break;
                                    }
                                }
                            }
                            if (pinned) {
                                objectsComplete = false;
                                seenIds.insert(*indexedId);
                                YT_TLOG_WARNING("Pinned file storage object failed reconciliation validation")
                                    .With("Path", object.GetPath());
                                continue;
                            }
                            TError error;
                            if (indexedId) {
                                auto result = Evict(*indexedId, &error);
                                if (result != EEvictionResult::Evicted) {
                                    objectsComplete = false;
                                    YT_TLOG_WARNING("Failed to quarantine indexed invalid file storage object")
                                        .With("Path", object.GetPath())
                                        .With(error);
                                }
                            } else if (!QuarantineUnknownPath(object, &error)) {
                                objectsComplete = false;
                                YT_TLOG_WARNING("Failed to quarantine invalid file storage object")
                                    .With("Path", object.GetPath())
                                    .With(error);
                            }
                            continue;
                        }
                        TFileStat stat(object, /*nofollow*/ true);
                        seenIds.insert(manifest->ObjectId);
                        adoptions.push_back({
                            .Id = TFileStorageObjectId(manifest->ObjectId),
                            .Digest = std::string(object.Basename()),
                            .Path = (object / "payload").GetPath(),
                            .Size = manifest->TotalSize,
                            .MTime = stat.MTime,
                            .MTimeNanoseconds = stat.MTimeNSec,
                        });
                    } catch (const std::exception& ex) {
                        objectsComplete = false;
                        YT_TLOG_WARNING("Failed to reconcile file storage object")
                            .With("Path", object.GetPath())
                            .With(ex);
                    }
                }
            } catch (const std::exception& ex) {
                objectsComplete = false;
                YT_TLOG_WARNING("Failed to reconcile file storage prefix")
                    .With("Path", prefix.GetPath())
                    .With(ex);
            }
        }

        Sort(adoptions, [] (const auto& lhs, const auto& rhs) {
            return std::tie(lhs.MTime, lhs.MTimeNanoseconds, lhs.Digest) <
                std::tie(rhs.MTime, rhs.MTimeNanoseconds, rhs.Digest);
        });

        {
            auto guard = Guard(Lock_);
            if (objectsComplete) {
                for (auto it = Objects_.begin(); it != Objects_.end();) {
                    if (!seenIds.contains(it->first)) {
                        auto entry = it->second;
                        if (entry->PinCount > 0) {
                            objectsComplete = false;
                            ++it;
                            continue;
                        }
                        if (entry->InLru) {
                            Lru_.erase(entry->LruIterator);
                            entry->InLru = false;
                        }
                        entry->Indexed = false;
                        auto toErase = it++;
                        Objects_.erase(toErase);
                    } else {
                        ++it;
                    }
                }
            }
            for (const auto& adoption : adoptions) {
                if (auto it = Objects_.find(adoption.Id.Underlying());
                    it != Objects_.end())
                {
                    if (it->second->Digest != adoption.Digest ||
                        it->second->Path != adoption.Path)
                    {
                        objectsComplete = false;
                    }
                    continue;
                }
                auto entry = std::make_shared<TObjectEntry>();
                entry->Id = adoption.Id;
                entry->Digest = adoption.Digest;
                entry->Path = adoption.Path;
                entry->Size = adoption.Size;
                entry->InLru = true;
                Lru_.push_back(entry->Id.Underlying());
                entry->LruIterator = std::prev(Lru_.end());
                Objects_.emplace(entry->Id.Underlying(), std::move(entry));
            }
        }
        if (!objectsComplete) {
            *complete = false;
        }
        RefreshMetrics();
    }

    void CleanupToSoftLimit(bool* complete)
    {
        THashSet<std::string> failedVictims;
        while (true) {
            std::string victim;
            {
                auto guard = Guard(Lock_);
                auto [totalSize, pinnedSize] = GetSizesLocked();
                Y_UNUSED(pinnedSize);
                if (totalSize <= Config_->SoftSizeLimit ||
                    Lru_.empty() ||
                    !TrashEntries_.empty() ||
                    !UnknownTrashEntries_.empty())
                {
                    return;
                }
                for (const auto& rawId : Lru_) {
                    if (!failedVictims.contains(rawId)) {
                        victim = rawId;
                        break;
                    }
                }
                if (victim.empty()) {
                    return;
                }
            }

            TError error;
            auto result = Evict(victim, &error);
            if (result == EEvictionResult::Failed) {
                *complete = false;
                failedVictims.insert(victim);
                YT_TLOG_WARNING("Failed to evict file storage object during cleanup")
                    .With(error);
            }
        }
    }

    std::pair<i64, i64> GetSizesLocked() const
    {
        i64 totalSize = TrashSize_;
        i64 pinnedSize = 0;
        for (const auto& [rawId, entry] : Objects_) {
            Y_UNUSED(rawId);
            totalSize += entry->Size;
            if (entry->PinCount > 0) {
                pinnedSize += entry->Size;
            }
        }
        return {totalSize, pinnedSize};
    }

    TMetricsSnapshot CaptureMetrics() const
    {
        auto guard = Guard(Lock_);
        auto [totalSize, pinnedSize] = GetSizesLocked();
        return {
            .Size = totalSize + ReservedSize_,
            .ObjectCount = std::ssize(Objects_),
            .PinnedSize = pinnedSize,
            .ReservedSize = ReservedSize_,
            .TrashSize = TrashSize_,
        };
    }

    void RefreshMetrics()
    {
        auto snapshot = CaptureMetrics();
        SizeGauge_.Update(snapshot.Size);
        ObjectCountGauge_.Update(snapshot.ObjectCount);
        PinnedSizeGauge_.Update(snapshot.PinnedSize);
        ReservedSizeGauge_.Update(snapshot.ReservedSize);
        TrashSizeGauge_.Update(snapshot.TrashSize);
        if (snapshot.PinnedSize > Config_->HardSizeLimit / 2) {
            PinnedOveruseError_->SetError(
                TError("Pinned file storage objects consume more than half of the hard limit")
                    .With("pinned_size", snapshot.PinnedSize)
                    .With("hard_size_limit", Config_->HardSizeLimit));
        } else {
            PinnedOveruseError_->ClearError();
        }
    }

    const TFileStorageConfigPtr Config_;
    const TFsPath Root_;
    std::unique_ptr<TFileLock> RootLock_;
    const IInvokerPtr Invoker_;
    const TLogger Logger_;
    const IStatusErrorStatePtr CapacityError_;
    const IStatusErrorStatePtr DiskFullError_;
    const IStatusErrorStatePtr PinnedOveruseError_;
    const IStatusErrorStatePtr StartupError_;
    const TGauge SizeGauge_;
    const TGauge ObjectCountGauge_;
    const TGauge PinnedSizeGauge_;
    const TGauge ReservedSizeGauge_;
    const TGauge TrashSizeGauge_;
    const TCounter HitCounter_;
    const TCounter MissCounter_;
    const TCounter FillFailureCounter_;
    const TCounter EvictionCounter_;
    const TCounter EvictedBytesCounter_;
    const TCounter RejectedPublicationCounter_;
    const TPeriodicExecutorPtr CleanupExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<std::string, TObjectEntryPtr> Objects_;
    THashMap<std::string, TInflightEntry> Inflight_;
    THashSet<std::string> ActiveStaging_;
    std::list<std::string> Lru_;
    THashMap<std::string, i64> TrashEntries_;
    THashSet<std::string> UnknownTrashEntries_;
    THashSet<std::string> UnknownRootEntries_;
    i64 ReservedSize_ = 0;
    i64 TrashSize_ = 0;
    const TAsyncSemaphorePtr UnknownFillSemaphore_ = New<TAsyncSemaphore>(1);
};

TFileStorageObject::TFileStorageObject(
    TWeakPtr<TFileStorage> owner,
    TObjectEntryPtr entry)
    : Owner_(std::move(owner))
    , Entry_(std::move(entry))
{ }

TFileStorageObject::~TFileStorageObject()
{
    if (auto owner = Owner_.Lock()) {
        owner->Release(Entry_);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoragePtr CreateFileStorage(
    TFileStorageConfigPtr config,
    IInvokerPtr invoker,
    TLogger logger,
    TProfiler profiler,
    IStatusProfilerPtr statusProfiler)
{
    return New<TFileStorage>(
        std::move(config),
        std::move(invoker),
        std::move(logger),
        std::move(profiler),
        std::move(statusProfiler));
}

TFuture<void> ReconcileFileStorageForTesting(const IFileStoragePtr& storage)
{
    auto* implementation = dynamic_cast<TFileStorage*>(storage.Get());
    THROW_ERROR_EXCEPTION_UNLESS(implementation, "Unknown file storage implementation");
    return implementation->ReconcileForTesting();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileStorage
