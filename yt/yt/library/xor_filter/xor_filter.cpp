#include "xor_filter.h"

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/digest/multi.h>

#include <queue>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TXorFilter::TXorFilter(TSharedRef data)
    : Data_(std::move(data))
{
    LoadMeta();
}

TXorFilter::TXorFilter(int bitsPerKey, int slotCount)
    : BitsPerKey_(bitsPerKey)
    , SlotCount_(slotCount)
    , MutableData_(TSharedMutableRef::Allocate(
        ComputeAllocationSize(SlotCount_, BitsPerKey_)))
    , Data_(MutableData_)
{
    if (bitsPerKey >= WordSize) {
        THROW_ERROR_EXCEPTION("Cannot create xor filter: expected bits_per_key < %v, got %v",
            WordSize,
            bitsPerKey);
    }

    for (int i = 0; i < 4; ++i) {
        Salts_[i] = RandomNumber<ui64>();
    }
}

bool TXorFilter::Contains(TFingerprint key) const
{
    ui64 actualXorFingerprint = 0;
    for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
        actualXorFingerprint ^= GetEntry(GetSlot(key, hashIndex));
    }
    return actualXorFingerprint == GetExpectedXorFingerprint(key);
}

TSharedRef TXorFilter::GetData() const
{
    return MutableData_ ? MutableData_ : Data_;
}

int TXorFilter::ComputeSlotCount(int keyCount)
{
    int slotCount = keyCount * LoadFactor + LoadFactorIncrement;

    // Make slotCount a multiple of 3.
    slotCount = slotCount / 3 * 3;

    return slotCount;
}

int TXorFilter::ComputeByteSize(int keyCount, int bitsPerKey)
{
    return DivCeil(ComputeSlotCount(keyCount) * bitsPerKey, WordSize) * sizeof(ui64);
}

int TXorFilter::ComputeAllocationSize(int slotCount, int bitsPerKey)
{
    int dataSize = DivCeil(slotCount * bitsPerKey, WordSize) * sizeof(ui64);
    return FormatVersionSize + dataSize + MetaSize;
}

ui64 TXorFilter::GetUi64Word(int index) const
{
    ui64 result;
    std::memcpy(
        &result,
        Data_.begin() + index * sizeof(result) + FormatVersionSize,
        sizeof(result));
    return result;
}

void TXorFilter::SetUi64Word(int index, ui64 value)
{
    std::memcpy(
        MutableData_.begin() + index * sizeof(value) + FormatVersionSize,
        &value,
        sizeof(value));
}

ui64 TXorFilter::GetHash(ui64 key, int hashIndex) const
{
    ui64 hash = Salts_[hashIndex];
    HashCombine(hash, key);
    return hash;
}

ui64 TXorFilter::GetEntry(int index) const
{
    // Fast path.
    if (BitsPerKey_ == 8) {
        return static_cast<ui8>(Data_[index + FormatVersionSize]);
    }

    int startBit = index * BitsPerKey_;
    int wordIndex = startBit / WordSize;
    int offset = startBit % WordSize;

    auto loWord = GetUi64Word(wordIndex);
    auto result = loWord >> offset;

    if (offset + BitsPerKey_ > WordSize) {
        auto hiWord = GetUi64Word(wordIndex + 1);
        result |= hiWord << (WordSize - offset);
    }

    return result & MaskLowerBits(BitsPerKey_);
}

void TXorFilter::SetEntry(int index, ui64 value)
{
    YT_ASSERT(MutableData_);

    // Fast path.
    if (BitsPerKey_ == 8) {
        MutableData_[index + FormatVersionSize] = static_cast<ui8>(value);
    }

    int startBit = index * BitsPerKey_;
    int wordIndex = startBit / WordSize;
    int offset = startBit % WordSize;

    auto loWord = GetUi64Word(wordIndex);
    loWord &= ~(MaskLowerBits(BitsPerKey_) << offset);
    loWord ^= value << offset;
    SetUi64Word(wordIndex, loWord);

    if (offset + BitsPerKey_ > WordSize) {
        auto hiWord = GetUi64Word(wordIndex + 1);
        hiWord &= ~(MaskLowerBits(BitsPerKey_) >> (WordSize - offset));
        hiWord ^= value >> (WordSize - offset);
        SetUi64Word(wordIndex + 1, hiWord);
    }
}

int TXorFilter::GetSlot(ui64 key, int hashIndex) const
{
    auto hash = GetHash(key, hashIndex);

    // A faster way to generate an almost uniform integer in [0, SlotCount_ / 3).
    // Note the "hash >> 32" part. Somehow higher 32 bits are distributed much
    // better than lower ones, and that turned out to be critical for the filter
    // building success probability.
    auto res = static_cast<ui64>((hash >> 32) * (SlotCount_ / 3)) >> 32;

    return res + (SlotCount_ / 3 * hashIndex);
}

ui64 TXorFilter::GetExpectedXorFingerprint(ui64 key) const
{
    return GetHash(key, 3) & MaskLowerBits(BitsPerKey_);
}

void TXorFilter::SaveMeta()
{
    {
        char* ptr = MutableData_.begin();
        WritePod(ptr, static_cast<i32>(FormatVersion));
    }

    {
        char* ptr = MutableData_.end() - MetaSize;
        WritePod(ptr, Salts_);
        WritePod(ptr, BitsPerKey_);
        WritePod(ptr, SlotCount_);
        YT_VERIFY(ptr == Data_.end());
    }
}

void TXorFilter::LoadMeta()
{
    int formatVersion;
    {
        const char* ptr = Data_.begin();
        ReadPod(ptr, formatVersion);
    }

    if (formatVersion != 1) {
        THROW_ERROR_EXCEPTION("Invalid XOR filter format version %v",
            formatVersion);
    }

    {
        const char* ptr = Data_.end() - MetaSize;
        ReadPod(ptr, Salts_);
        ReadPod(ptr, BitsPerKey_);
        ReadPod(ptr, SlotCount_);
        YT_VERIFY(ptr == Data_.end());
    }
}

TXorFilterPtr TXorFilter::Build(TRange<TFingerprint> keys, int bitsPerKey, int trialCount)
{
    for (int trialIndex = 0; trialIndex < trialCount; ++trialIndex) {
        if (auto filter = DoBuild(keys, bitsPerKey)) {
            return filter;
        }
    }

    THROW_ERROR_EXCEPTION("Failed to build XOR filter in %v attempts",
        trialCount);
}

TXorFilterPtr TXorFilter::DoBuild(TRange<TFingerprint> keys, int bitsPerKey)
{
    int slotCount = ComputeSlotCount(std::ssize(keys));
    auto filter = New<TXorFilter>(bitsPerKey, slotCount);

    std::vector<int> assignedKeysXor(slotCount);
    std::vector<int> hitCount(slotCount);

    for (auto [keyIndex, key] : Enumerate(keys)) {
        for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
            int slot = filter->GetSlot(key, hashIndex);
            assignedKeysXor[slot] ^= keyIndex;
            ++hitCount[slot];
        }
    }

    std::vector<char> inQueue(slotCount);
    std::queue<int> queue;
    for (int slot = 0; slot < slotCount; ++slot) {
        if (hitCount[slot] == 1) {
            queue.push(slot);
            inQueue[slot] = true;
        }
    }

    std::vector<std::pair<int, int>> order;
    order.reserve(keys.Size());

    while (!queue.empty()) {
        int candidateSlot = queue.front();
        queue.pop();

        if (hitCount[candidateSlot] == 0) {
            continue;
        }

        YT_VERIFY(hitCount[candidateSlot] == 1);

        int keyIndex = assignedKeysXor[candidateSlot];
        YT_VERIFY(keyIndex != -1);
        order.emplace_back(keyIndex, candidateSlot);

        auto key = keys[keyIndex];
        for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
            int slot = filter->GetSlot(key, hashIndex);
            assignedKeysXor[slot] ^= keyIndex;
            if (--hitCount[slot] == 1 && !inQueue[slot]) {
                inQueue[slot] = true;
                queue.push(slot);
            }
        }
    }

    if (std::ssize(order) < std::ssize(keys)) {
        return nullptr;
    }

    std::reverse(order.begin(), order.end());

    for (auto [keyIndex, candidateSlot] : order) {
        auto key = keys[keyIndex];

        YT_VERIFY(filter->GetEntry(candidateSlot) == 0);

        ui64 expectedXor = filter->GetExpectedXorFingerprint(key);
        ui64 actualXor = 0;

        for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
            actualXor ^= filter->GetEntry(filter->GetSlot(key, hashIndex));
        }

        filter->SetEntry(candidateSlot, actualXor ^ expectedXor);
    }

    filter->SaveMeta();

    return filter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
