#include "min_hash_digest.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TMinHashDigest::TMinHashDigest(IMemoryUsageTrackerPtr memoryTracker)
    : MemoryTracker_(std::move(memoryTracker))
{ }

TMinHashDigest::~TMinHashDigest()
{
    if (MemoryTracker_) {
        MemoryTracker_->Release(GetWeight());
    }
}

bool TMinHashDigest::IsInitialized() const
{
    return Initialized_;
}

void TMinHashDigest::Initialize(TSharedRef data)
{
    YT_VERIFY(!IsInitialized());

    Initialized_ = true;

    const char* ptr = data.begin();

    i32 formatVersion;
    ReadPod(ptr, formatVersion);
    if (formatVersion != 1) {
        THROW_ERROR_EXCEPTION("Invalid min hash digest format version %v",
            formatVersion);
    }

    i32 writeMinHashSize;
    i32 deleteTombstoneMinHashSize;

    ReadPod(ptr, writeMinHashSize);
    ReadPod(ptr, deleteTombstoneMinHashSize);

    for (int index = 0; index < writeMinHashSize; ++index) {
        std::pair<TFingerprint, ui64> element;
        ReadPod(ptr, element);
        WriteMinHashes_.insert(std::move(element));
    }

    for (int index = 0; index < deleteTombstoneMinHashSize; ++index) {
        std::pair<TFingerprint, ui64> element;
        ReadPod(ptr, element);
        DeleteTombstoneMinHashes_.insert(element);
    }

    if (MemoryTracker_) {
        // NB(dave11ar): We can acquire after initialization because tablet_background is limitless.
        MemoryTracker_->Acquire(GetWeight());
    }
}

i64 TMinHashDigest::GetWeight() const
{
    return sizeof(i32) * 3 +
        (WriteMinHashes_.size() + DeleteTombstoneMinHashes_.size()) * (sizeof(TFingerprint) + sizeof(ui64));
}

TSharedRef TMinHashDigest::Build(
    const std::map<TFingerprint, ui64>& writeMinHashes,
    const std::map<TFingerprint, ui64>& deleteTombstoneMinHashes)
{
    i32 allocationSize = sizeof(i32) * 3 +
        (writeMinHashes.size() + deleteTombstoneMinHashes.size()) * (sizeof(TFingerprint) + sizeof(ui64));

    auto data = TSharedMutableRef::Allocate(allocationSize);
    char* ptr = data.begin();

    WritePod(ptr, FormatVersion);

    WritePod(ptr, static_cast<i32>(writeMinHashes.size()));
    WritePod(ptr, static_cast<i32>(deleteTombstoneMinHashes.size()));

    for (auto element : writeMinHashes) {
        WritePod(ptr, element);
    }

    for (auto element : deleteTombstoneMinHashes) {
        WritePod(ptr, element);
    }

    return data;
}

void Serialize(const TMinHashDigest& minHashDigest, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("initialized").Value(minHashDigest.IsInitialized())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
