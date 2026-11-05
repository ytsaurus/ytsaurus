#include "memory_profile_retention.h"

#include "config.h"

#include <yt/yt/core/misc/fs.h>

#include <util/system/fstat.h>

#include <algorithm>
#include <limits>
#include <map>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IncompleteOomMemoryProfileManifestFileExtension = ".yson_incomplete";

struct TProfileFile
{
    std::string Name;
    std::string Path;
    TInstant ModificationTime;
    i64 Size = 0;
};

struct TProfileDump
{
    std::string Suffix;
    const TProfileFile* CurrentProfile = nullptr;
    const TProfileFile* PeakProfile = nullptr;
    const TProfileFile* Manifest = nullptr;
    i64 TotalSize = 0;
};

std::optional<TStringBuf> TryGetSuffix(
    TStringBuf fileName,
    TStringBuf prefix,
    TStringBuf extension,
    const std::optional<std::string>& filenameSuffix)
{
    if (!fileName.StartsWith(prefix) || !fileName.EndsWith(extension)) {
        return std::nullopt;
    }

    auto suffix = fileName.SubStr(prefix.size(), fileName.size() - prefix.size() - extension.size());
    if (suffix.empty()) {
        return std::nullopt;
    }

    if (filenameSuffix) {
        auto configuredSuffix = TStringBuf(*filenameSuffix);
        if (!suffix.StartsWith(configuredSuffix) ||
            suffix.size() == configuredSuffix.size() ||
            suffix[configuredSuffix.size()] != '_')
        {
            return std::nullopt;
        }
    }

    auto expectedSuffixSize = filenameSuffix ? filenameSuffix->size() + 1 : 0;
    if (suffix.SubStr(expectedSuffixSize).find('_') != TStringBuf::npos) {
        return std::nullopt;
    }

    return suffix;
}

bool IsMemoryProfileFile(
    const std::string& fileName,
    const std::optional<std::string>& filenameSuffix)
{
    return
        TryGetSuffix(fileName, CurrentMemoryProfileFilePrefix, MemoryProfileFileExtension, filenameSuffix) ||
        TryGetSuffix(fileName, CurrentMemoryProfileFilePrefix, IncompleteMemoryProfileFileExtension, filenameSuffix) ||
        TryGetSuffix(fileName, PeakMemoryProfileFilePrefix, MemoryProfileFileExtension, filenameSuffix) ||
        TryGetSuffix(fileName, PeakMemoryProfileFilePrefix, IncompleteMemoryProfileFileExtension, filenameSuffix) ||
        TryGetSuffix(
            fileName,
            OomMemoryProfileManifestFilePrefix,
            IncompleteOomMemoryProfileManifestFileExtension,
            filenameSuffix) ||
        TryGetSuffix(
            fileName,
            OomMemoryProfileManifestFilePrefix,
            OomMemoryProfileManifestFileExtension,
            filenameSuffix);
}

i64 AddSizes(i64 lhs, i64 rhs)
{
    if (rhs > std::numeric_limits<i64>::max() - lhs) {
        return std::numeric_limits<i64>::max();
    }
    return lhs + rhs;
}

bool RemoveFile(const TProfileFile& file, TMemoryProfileCleanupResult* result, bool orphan)
{
    try {
        NFS::Remove(file.Path);
    } catch (const std::exception&) {
        // Best effort: the file may have been removed concurrently.
        ++result->FailedRemovalCount;
        return false;
    }
    result->RemovedByteCount = AddSizes(result->RemovedByteCount, file.Size);
    if (orphan) {
        ++result->RemovedOrphanFileCount;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMemoryProfileCleanupResult CleanupMemoryProfiles(
    const std::string& path,
    const std::optional<std::string>& filenameSuffix,
    const TMemoryProfileRetentionConfigPtr& retentionConfig,
    TInstant now)
{
    std::map<std::string, TProfileFile> files;
    for (const auto& fileName : NFS::EnumerateFiles(path)) {
        if (!IsMemoryProfileFile(fileName, filenameSuffix)) {
            continue;
        }

        auto filePath = NFS::CombinePaths(path, fileName);
        TFileStat fileStat(TString(filePath), /*nofollow*/ true);
        if (!fileStat.IsFile()) {
            continue;
        }

        files.emplace(fileName, TProfileFile{
            .Name = fileName,
            .Path = std::move(filePath),
            .ModificationTime = TInstant::Seconds(fileStat.MTime),
            .Size = static_cast<i64>(std::min<ui64>(fileStat.Size, std::numeric_limits<i64>::max())),
        });
    }

    std::vector<TProfileDump> dumps;
    for (const auto& [fileName, manifest] : files) {
        auto suffix = TryGetSuffix(
            fileName,
            OomMemoryProfileManifestFilePrefix,
            OomMemoryProfileManifestFileExtension,
            filenameSuffix);
        if (!suffix) {
            continue;
        }

        auto currentIt = files.find(Format(
            "%v%v%v",
            CurrentMemoryProfileFilePrefix,
            *suffix,
            MemoryProfileFileExtension));
        auto peakIt = files.find(Format(
            "%v%v%v",
            PeakMemoryProfileFilePrefix,
            *suffix,
            MemoryProfileFileExtension));
        if (currentIt == files.end() || peakIt == files.end()) {
            continue;
        }

        dumps.push_back(TProfileDump{
            .Suffix = std::string(*suffix),
            .CurrentProfile = &currentIt->second,
            .PeakProfile = &peakIt->second,
            .Manifest = &manifest,
            .TotalSize = AddSizes(AddSizes(currentIt->second.Size, peakIt->second.Size), manifest.Size),
        });
    }

    std::sort(dumps.begin(), dumps.end(), [] (const TProfileDump& lhs, const TProfileDump& rhs) {
        return std::tie(lhs.Manifest->ModificationTime, lhs.Suffix) >
            std::tie(rhs.Manifest->ModificationTime, rhs.Suffix);
    });

    TMemoryProfileCleanupResult result;
    int retainedDumpCount = 0;
    i64 retainedTotalSize = 0;
    THashSet<std::string> accountedFiles;

    for (const auto& dump : dumps) {
        accountedFiles.insert(dump.CurrentProfile->Name);
        accountedFiles.insert(dump.PeakProfile->Name);
        accountedFiles.insert(dump.Manifest->Name);

        bool expired = retentionConfig->MaxDumpAge &&
            dump.Manifest->ModificationTime < now &&
            now - dump.Manifest->ModificationTime > *retentionConfig->MaxDumpAge;
        bool exceedsCount = retentionConfig->MaxDumpCount &&
            retainedDumpCount >= *retentionConfig->MaxDumpCount;
        bool exceedsTotalSize = retentionConfig->MaxTotalSize &&
            AddSizes(retainedTotalSize, dump.TotalSize) > *retentionConfig->MaxTotalSize;
        bool exceedsCountOrSize = retainedDumpCount > 0 && (exceedsCount || exceedsTotalSize);

        if (expired || exceedsCountOrSize) {
            bool removed = RemoveFile(*dump.CurrentProfile, &result, /*orphan*/ false);
            removed &= RemoveFile(*dump.PeakProfile, &result, /*orphan*/ false);
            removed &= RemoveFile(*dump.Manifest, &result, /*orphan*/ false);
            if (removed) {
                ++result.RemovedDumpCount;
            }
        } else {
            ++retainedDumpCount;
            retainedTotalSize = AddSizes(retainedTotalSize, dump.TotalSize);
        }
    }

    for (const auto& [fileName, file] : files) {
        if (!accountedFiles.contains(fileName) &&
            file.ModificationTime < now &&
            now - file.ModificationTime > retentionConfig->MaxOrphanAge)
        {
            RemoveFile(file, &result, /*orphan*/ true);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
