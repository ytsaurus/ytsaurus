#include "chunk_index_read_controller.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_column_mapping.h"
#include "versioned_block_reader.h"

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/misc/algorithm_helpers.h>
#include <yt/yt/core/misc/checksum.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct THashTableChunkIndexReadControllerTag
{ };

DEFINE_ENUM(EKeyRequestState,
    (PendingIndexData)
    (PendingRowData)
    (Finished)
);

////////////////////////////////////////////////////////////////////////////////

class THashTableChunkIndexReadController
    : public IChunkIndexReadController
{
public:
    THashTableChunkIndexReadController(
        TChunkId chunkId,
        TCachedVersionedChunkMetaPtr chunkMeta,
        const TColumnFilter& columnFilter,
        TSharedRange<TLegacyKey> keys,
        TKeyComparer keyComparer,
        const TTableSchemaPtr& tableSchema,
        TTimestamp timestamp,
        bool produceAllVersions,
        IBlockCachePtr blockCache,
        std::optional<TChunkIndexReadControllerTestingOptions> testingOptions,
        const TLogger& logger)
        : ChunkId_(chunkId)
        , ChunkMeta_(std::move(chunkMeta))
        , Keys_(std::move(keys))
        , KeyComparer_(std::move(keyComparer))
        , BlockCache_(std::move(blockCache))
        , GroupReorderingEnabled_(false)
        , SchemaIdMapping_(TChunkColumnMapping(tableSchema, ChunkMeta_->ChunkSchema())
            .BuildVersionedSimpleSchemaIdMapping(columnFilter))
        , GroupCount_(
            ChunkMeta_->HashTableChunkIndexMeta()->IndexedBlockFormatDetail.GetGroupCount())
        , GroupIndexesToRead_(
            ChunkMeta_->HashTableChunkIndexMeta()->IndexedBlockFormatDetail.GetGroupIndexesToRead(SchemaIdMapping_))
        , TestingOptions_(std::move(testingOptions))
        , Logger(logger.WithTag("ChunkId: %v",
            ChunkId_))
        , MemoryPool_(THashTableChunkIndexReadControllerTag{})
    {
        RowReader_.emplace(
            tableSchema->GetKeyColumnCount(),
            SchemaIdMapping_,
            timestamp,
            produceAllVersions,
            ChunkMeta_->ChunkSchema(),
            GroupIndexesToRead_);

        auto chunkKeyColumnCount = ChunkMeta_->GetChunkKeyColumnCount();
        const auto& chunkIndexBlockMetas = ChunkMeta_->HashTableChunkIndexMeta()->ChunkIndexBlockMetas;

        KeyRequests_.reserve(Keys_.size());
        Result_.resize(Keys_.size());

        for (int keyIndex = 0; keyIndex < std::ssize(Keys_); ++keyIndex) {
            const auto& key = Keys_[keyIndex];

            auto metaIndex = BinarySearch(
                0,
                std::ssize(chunkIndexBlockMetas),
                [&] (int blockIndex) {
                    return CompareWithWidening(
                        ToKeyRef(chunkIndexBlockMetas[blockIndex].BlockLastKey, chunkKeyColumnCount),
                        ToKeyRef(key)) < 0;
                });

            if (metaIndex == chunkIndexBlockMetas.size()) {
                // Key is missing.
                KeyRequests_.emplace_back();
                continue;
            }

            auto startSlotIndex = TestingOptions_ && !TestingOptions_->KeySlotIndexes.empty()
                ? std::make_optional(TestingOptions_->KeySlotIndexes[keyIndex])
                : std::nullopt;

            YT_ASSERT(static_cast<int>(key.GetCount()) >= chunkKeyColumnCount);
            auto& request = KeyRequests_.emplace_back(
                chunkIndexBlockMetas[metaIndex].BlockIndex,
                GetFarmFingerprint(MakeRange(key.Begin(), chunkKeyColumnCount)),
                startSlotIndex,
                &chunkIndexBlockMetas[metaIndex].FormatDetail);

            if (GroupCount_ > 1) {
                request.GroupOffsets.resize(GroupCount_);
                if (GroupReorderingEnabled_) {
                    request.GroupIndexes.resize(GroupCount_);
                }
            }

            HandleKeyRequest(&request);
        }

        YT_VERIFY(KeyRequests_.size() == Result_.size());

        BlockIndexToData_.clear();

        MakePlanForPendingSectors();

        OnRequestsGenerated();
    }

    TReadRequest GetReadRequest() override
    {
        YT_VERIFY(!IsFinished());

        auto request = std::move(ReadRequest_);
        return request;
    }

    const std::vector<TVersionedRow>& GetResult() const override
    {
        YT_VERIFY(IsFinished());

        return Result_;
    }

    void HandleReadResponse(TReadResponse response) override
    {
        for (int pendingSectorIndex = 0; pendingSectorIndex < std::ssize(PendingSectorAddresses_); ++pendingSectorIndex) {
            auto address = PendingSectorAddresses_[pendingSectorIndex];
            auto descriptor = PendingSectorDescriptors_[pendingSectorIndex];

            YT_VERIFY(descriptor.SubrequestIndex < std::ssize(response.Fragments));
            YT_VERIFY(response.Fragments[descriptor.SubrequestIndex]);
            YT_VERIFY(descriptor.SubrequestOffset + THashTableChunkIndexFormatDetail::SectorSize <=
                std::ssize(response.Fragments[descriptor.SubrequestIndex]));
            auto sectorData = TRef(response.Fragments[descriptor.SubrequestIndex]).Slice(
                descriptor.SubrequestOffset,
                descriptor.SubrequestOffset + THashTableChunkIndexFormatDetail::SectorSize);

            TChecksum expectedChecksum;
            memcpy(&expectedChecksum, sectorData.End() - sizeof(TChecksum), sizeof(TChecksum));

            auto refWithoutChecksum = sectorData.Slice(
                0,
                THashTableChunkIndexFormatDetail::SectorSize - sizeof(TChecksum));
            auto actualChecksum = GetChecksum(refWithoutChecksum);
            if (expectedChecksum != actualChecksum) {
                THROW_ERROR_EXCEPTION("Chunk index sector checksum mismatch")
                    << TErrorAttribute("expected_checksum", expectedChecksum)
                    << TErrorAttribute("actual_checksum", actualChecksum)
                    << TErrorAttribute("recalculated_checksum", GetChecksum(refWithoutChecksum));
            }

            PendingSectorAddressToData_[address] = sectorData;
        }

        PendingSectorAddresses_.clear();
        PendingSectorDescriptors_.clear();

        YT_VERIFY(PendingBlockIndexes_.size() == response.SystemBlocks.size());
        for (int index = 0; index < std::ssize(PendingBlockIndexes_); ++index) {
            auto block = std::move(response.SystemBlocks[index]);
            YT_VERIFY(BlockCache_);
            BlockCache_->PutBlock(
                TBlockId(ChunkId_, PendingBlockIndexes_[index]),
                EBlockType::HashTableChunkIndex,
                block);
            EmplaceOrCrash(BlockIndexToData_, PendingBlockIndexes_[index], std::move(block.Data));
        }

        PendingBlockIndexes_.clear();

        for (int requestIndex = 0; requestIndex < std::ssize(KeyRequests_); ++requestIndex) {
            auto& keyRequest = KeyRequests_[requestIndex];
            switch (keyRequest.State) {
                case EKeyRequestState::PendingIndexData:
                    HandleKeyRequest(&keyRequest);
                    break;

                case EKeyRequestState::PendingRowData:
                    YT_VERIFY(keyRequest.LastSubrequestIndex <= std::ssize(response.Fragments));
                    keyRequest.RowData.reserve(keyRequest.LastSubrequestIndex - keyRequest.FirstSubrequestIndex);
                    for (auto index = keyRequest.FirstSubrequestIndex; index < keyRequest.LastSubrequestIndex; ++index) {
                        YT_VERIFY(response.Fragments[index]);
                        keyRequest.RowData.push_back(std::move(response.Fragments[index]));
                    }

                    HandleRowData(requestIndex, &keyRequest);

                    break;

                case EKeyRequestState::Finished:
                    break;

                default:
                    YT_ABORT();
            }
        }

        PendingSectorAddressToData_.clear();
        BlockIndexToData_.clear();

        MakePlanForPendingSectors();

        OnRequestsGenerated();
    }

    bool IsFinished() const override
    {
        return ReadRequest_.SystemBlockIndexes.empty() && ReadRequest_.FragmentSubrequests.empty();
    }

private:
    struct TKeyRequest
    {
        TKeyRequest()
        { }

        TKeyRequest(
            int blockIndex,
            TFingerprint fingerprint,
            std::optional<int> startSlotIndex,
            const THashTableChunkIndexFormatDetail* formatDetail)
            : HashTableBlockIndex(blockIndex)
            , FormatDetail(formatDetail)
            , SerializableFingerprint(FormatDetail->GetSerializableFingerprint(fingerprint))
            , StartSlotIndex(startSlotIndex.value_or(FormatDetail->GetStartSlotIndex(fingerprint)))
            , CurrentSlotIndex(StartSlotIndex)
            , State(EKeyRequestState::PendingIndexData)
        { }

        const int HashTableBlockIndex = -1;
        // NB: ChunkMeta_ holds this.
        const THashTableChunkIndexFormatDetail* FormatDetail = nullptr;

        const THashTableChunkIndexFormatDetail::TSerializableFingerprint SerializableFingerprint =
            THashTableChunkIndexFormatDetail::MissingEntryFingerprint;
        const int StartSlotIndex = -1;

        int CurrentSlotIndex = -1;

        EKeyRequestState State = EKeyRequestState::Finished;

        int FirstSubrequestIndex = -1;
        int LastSubrequestIndex = -1;

        std::vector<int> GroupOffsets;
        std::vector<int> GroupIndexes;

        TCompactVector<TSharedRef, IndexedRowTypicalGroupCount> RowData;
    };

    struct TSectorAddress
    {
        int BlockIndex;
        int SectorIndex;

        bool operator == (const TSectorAddress& other) const
        {
            return BlockIndex == other.BlockIndex &&
                SectorIndex == other.SectorIndex;
        }

        bool operator < (const TSectorAddress& other) const
        {
            return BlockIndex < other.BlockIndex ||
                (BlockIndex == other.BlockIndex && SectorIndex < other.SectorIndex);
        }

        explicit operator size_t() const
        {
            return MultiHash(BlockIndex, SectorIndex);
        }
    };

    struct TPendingSectorDescriptor
    {
        int SubrequestIndex;
        int SubrequestOffset;
    };


    const TChunkId ChunkId_;
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const TSharedRange<TLegacyKey> Keys_;
    const TKeyComparer KeyComparer_;
    const IBlockCachePtr BlockCache_;
    const bool GroupReorderingEnabled_;
    const std::vector<TColumnIdMapping> SchemaIdMapping_;
    const int GroupCount_;
    const std::vector<int> GroupIndexesToRead_;
    const std::optional<TChunkIndexReadControllerTestingOptions> TestingOptions_;

    const TLogger Logger;

    std::optional<TIndexedVersionedRowReader> RowReader_;
    TChunkedMemoryPool MemoryPool_;

    std::vector<TKeyRequest> KeyRequests_;

    // NB: These three fields are used only when no block cache is provided.
    // NB: Chunk index entry requests are grouped (and coalesced) by corresponding sector addresses.
    std::vector<TSectorAddress> PendingSectorAddresses_;
    std::vector<TPendingSectorDescriptor> PendingSectorDescriptors_;
    THashMap<TSectorAddress, TRef> PendingSectorAddressToData_;

    // NB: These two fields are used only when block cache is provided.
    std::vector<int> PendingBlockIndexes_;
    THashMap<int, TSharedRef> BlockIndexToData_;

    TReadRequest ReadRequest_;

    std::vector<TVersionedRow> Result_;


    void HandleKeyRequest(TKeyRequest* request)
    {
        while (request->State == EKeyRequestState::PendingIndexData) {
            TSectorAddress sectorAddress{
                .BlockIndex = request->HashTableBlockIndex,
                .SectorIndex = request->FormatDetail->GetSectorIndex(request->CurrentSlotIndex),
            };

            if (BlockCache_) {
                auto handleBlockData = [&] (const TSharedRef& blockData) {
                    YT_VERIFY(blockData.Size() % THashTableChunkIndexFormatDetail::SectorSize == 0);
                    auto sector = TRef(blockData).Slice(
                        sectorAddress.SectorIndex * THashTableChunkIndexFormatDetail::SectorSize,
                        (sectorAddress.SectorIndex + 1) * THashTableChunkIndexFormatDetail::SectorSize);
                    HandleChunkIndexSector(request, sector, sectorAddress.SectorIndex);
                };

                auto it = BlockIndexToData_.find(sectorAddress.BlockIndex);
                if (it != BlockIndexToData_.end()) {
                    YT_VERIFY(it->second);
                    handleBlockData(it->second);
                } else {
                    // NB: We simply access block cache synchronously here.
                    // Asynchronous read may be performed on underlying reading layer.
                    auto blockData = BlockCache_->FindBlock(
                        TBlockId(ChunkId_, sectorAddress.BlockIndex),
                        EBlockType::HashTableChunkIndex)
                        .Data;
                    if (blockData) {
                        handleBlockData(blockData);
                        BlockIndexToData_[sectorAddress.BlockIndex] = std::move(blockData);
                    } else {
                        PendingBlockIndexes_.push_back(sectorAddress.BlockIndex);
                        break;
                    }
                }
            } else {
                auto sectorDataIt = PendingSectorAddressToData_.find(sectorAddress);
                if (sectorDataIt != PendingSectorAddressToData_.end()) {
                    HandleChunkIndexSector(request, sectorDataIt->second, sectorAddress.SectorIndex);
                } else {
                    PendingSectorAddresses_.push_back(sectorAddress);
                    break;
                }
            }
        }
    }

    void HandleChunkIndexSector(TKeyRequest* request, TRef sector, int sectorIndex)
    {
        auto minSectorSlotIndex = request->FormatDetail->GetSlotCountInSector() * sectorIndex;
        YT_VERIFY(sectorIndex < request->FormatDetail->GetSectorCount());
        YT_VERIFY(request->CurrentSlotIndex >= minSectorSlotIndex);

        while (
            request->State == EKeyRequestState::PendingIndexData &&
            request->FormatDetail->GetSectorIndex(request->CurrentSlotIndex) == sectorIndex)
        {
            auto offsetInSector = (request->CurrentSlotIndex - minSectorSlotIndex) *
                request->FormatDetail->GetEntryByteSize();
            YT_VERIFY(offsetInSector >= 0 && offsetInSector < THashTableChunkIndexFormatDetail::SectorSize);

            auto* entryData = sector.Begin() + offsetInSector;

            THashTableChunkIndexFormatDetail::TSerializableFingerprint slotFingerprint;
            ReadPod(entryData, slotFingerprint);
            auto keyFingerprint = request->SerializableFingerprint;

            if (TestingOptions_ && TestingOptions_->FingerprintDomainSize) {
                slotFingerprint = request->FormatDetail->NarrowFingerprint(
                    slotFingerprint,
                    *TestingOptions_->FingerprintDomainSize);
                keyFingerprint = request->FormatDetail->NarrowFingerprint(
                    keyFingerprint,
                    *TestingOptions_->FingerprintDomainSize);
            }

            if (slotFingerprint == keyFingerprint) {
                int blockIndex;
                ReadPod(entryData, blockIndex);
                i64 blockOffset;
                ReadPod(entryData, blockOffset);
                i64 length;
                ReadPod(entryData, length);

                auto& groupOffsets = request->GroupOffsets;
                auto& groupIndexes = request->GroupIndexes;

                if (GroupCount_ > 1) {
                    for (int i = 0; i < GroupCount_; ++i) {
                        ReadPod(entryData, groupOffsets[i]);
                    }
                    if (GroupReorderingEnabled_) {
                        for (int i = 0; i < GroupCount_; ++i) {
                            ReadPod(entryData, groupIndexes[i]);
                        }
                    }
                }

                request->FirstSubrequestIndex = std::ssize(ReadRequest_.FragmentSubrequests);
                if (GroupIndexesToRead_.empty()) {
                    request->LastSubrequestIndex = request->FirstSubrequestIndex + 1;
                    ReadRequest_.FragmentSubrequests.push_back(TChunkFragmentRequest{
                        .ChunkId = ChunkId_,
                        .Length = length,
                        .BlockIndex = blockIndex,
                        .BlockOffset = blockOffset,
                    });
                } else {
                    request->LastSubrequestIndex = request->FirstSubrequestIndex + std::ssize(GroupIndexesToRead_) + 1;

                    ReadRequest_.FragmentSubrequests.push_back(TChunkFragmentRequest{
                        .ChunkId = ChunkId_,
                        .Length = groupOffsets[0],
                        .BlockIndex = blockIndex,
                        .BlockOffset = blockOffset,
                    });

                    for (auto logicalGroupIndex : GroupIndexesToRead_) {
                        auto physicalGroupIndex = !groupIndexes.empty()
                            ? groupIndexes[logicalGroupIndex]
                            : logicalGroupIndex;
                        // NB: Avoid row checksum at the end of the row.
                        auto nextGroupOffset = physicalGroupIndex + 1 == GroupCount_
                            ? static_cast<int>(length - sizeof(TChecksum))
                            : groupOffsets[physicalGroupIndex + 1];
                        ReadRequest_.FragmentSubrequests.push_back(TChunkFragmentRequest{
                            .ChunkId = ChunkId_,
                            .Length = nextGroupOffset - groupOffsets[physicalGroupIndex],
                            .BlockIndex = blockIndex,
                            .BlockOffset = blockOffset + groupOffsets[physicalGroupIndex],
                        });
                    }
                }

                request->State = EKeyRequestState::PendingRowData;
            } else if (request->FormatDetail->IsEntryPresent(slotFingerprint)) {
                request->CurrentSlotIndex = request->FormatDetail->GetNextSlotIndex(request->CurrentSlotIndex);
                if (request->CurrentSlotIndex == request->StartSlotIndex) {
                    // Key is missing.
                    request->State = EKeyRequestState::Finished;
                }
            } else {
                // Key is missing.
                request->State = EKeyRequestState::Finished;
            }
        }
    }

    void HandleRowData(int requestIndex, TKeyRequest* request)
    {
        auto row = RowReader_->ProcessAndGetRow(
            request->RowData,
            request->GroupOffsets.data(),
            request->GroupIndexes.data(),
            &MemoryPool_);

        if (row &&
            CompareKeys(
                row.Keys(),
                ToKeyRef(Keys_[requestIndex]),
                KeyComparer_) == 0)
        {
            request->State = EKeyRequestState::Finished;
            Result_[requestIndex] = row;
        } else {
            request->CurrentSlotIndex = request->FormatDetail->GetNextSlotIndex(request->CurrentSlotIndex);
            if (request->CurrentSlotIndex == request->StartSlotIndex) {
                // Key is missing.
                request->State = EKeyRequestState::Finished;
            } else {
                request->State = EKeyRequestState::PendingIndexData;
                HandleKeyRequest(request);
            }
            request->RowData.clear();
        }
    }

    void MakePlanForPendingSectors()
    {
        if (BlockCache_) {
            RequestBlocksForPendingSectors();
        } else {
            RequestFragmentsForPendingSectors();
        }
    }

    void RequestBlocksForPendingSectors()
    {
        SortUnique(PendingBlockIndexes_);
        ReadRequest_.SystemBlockIndexes = PendingBlockIndexes_;
    }

    void RequestFragmentsForPendingSectors()
    {
        SortUnique(PendingSectorAddresses_);

        struct TPendingSectorRange
        {
            const int BlockIndex;
            const int BeginSectorIndex;

            int EndSectorIndex;

            TPendingSectorRange(TSectorAddress sectorAddress)
                : BlockIndex(sectorAddress.BlockIndex)
                , BeginSectorIndex(sectorAddress.SectorIndex)
                , EndSectorIndex(BeginSectorIndex + 1)
            { }
        };

        std::optional<TPendingSectorRange> currentSectorRange;

        auto flushPendingSectorRange = [&] {
            auto sectorCount = currentSectorRange->EndSectorIndex - currentSectorRange->BeginSectorIndex;
            ReadRequest_.FragmentSubrequests.push_back(TChunkFragmentRequest{
                .ChunkId = ChunkId_,
                .Length = sectorCount * THashTableChunkIndexFormatDetail::SectorSize,
                .BlockIndex = currentSectorRange->BlockIndex,
                .BlockOffset = currentSectorRange->BeginSectorIndex * THashTableChunkIndexFormatDetail::SectorSize,
            });

            currentSectorRange.reset();
        };

        PendingSectorDescriptors_.reserve(PendingSectorAddresses_.size());

        for (auto sectorAddress : PendingSectorAddresses_) {
            if (!currentSectorRange) {
                currentSectorRange.emplace(sectorAddress);
            } else if (
                currentSectorRange->BlockIndex != sectorAddress.BlockIndex ||
                currentSectorRange->EndSectorIndex < sectorAddress.SectorIndex)
            {
                flushPendingSectorRange();

                currentSectorRange.emplace(sectorAddress);
            } else {
                currentSectorRange->EndSectorIndex = sectorAddress.SectorIndex + 1;
            }

            auto sectorIndexInSubrequest = sectorAddress.SectorIndex - currentSectorRange->BeginSectorIndex;
            PendingSectorDescriptors_.push_back(TPendingSectorDescriptor{
                .SubrequestIndex = static_cast<int>(ReadRequest_.FragmentSubrequests.size()),
                .SubrequestOffset = static_cast<int>(sectorIndexInSubrequest * THashTableChunkIndexFormatDetail::SectorSize)
            });
        }

        if (currentSectorRange) {
            flushPendingSectorRange();
        }
    }

    void OnRequestsGenerated() const
    {
        if (IsFinished()) {
            // Read session is finished now.
            YT_LOG_DEBUG("Hash table chunk index read contoller has no new requests");
            return;
        }

        int fragmentsSize = 0;
        for (const auto& request : ReadRequest_.FragmentSubrequests) {
            fragmentsSize += request.Length;
        }

        YT_LOG_DEBUG("Hash table chunk index read contoller generated new requests "
            "(FragmentCount: %v, FragmentsSize: %v, SystemBlockCount: %v, RequestedHashIndexSectorCount: %v)",
            ReadRequest_.FragmentSubrequests.size(),
            fragmentsSize,
            ReadRequest_.SystemBlockIndexes.size(),
            PendingSectorDescriptors_.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkIndexReadControllerPtr CreateChunkIndexReadController(
    TChunkId chunkId,
    const TColumnFilter& columnFilter,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TSharedRange<TLegacyKey> keys,
    TKeyComparer keyComparer,
    const TTableSchemaPtr& tableSchema,
    TTimestamp timestamp,
    bool produceAllVersions,
    const IBlockCachePtr& blockCache,
    std::optional<TChunkIndexReadControllerTestingOptions> testingOptions,
    const TLogger& logger)
{
    YT_VERIFY(chunkMeta->HashTableChunkIndexMeta());

    auto actualBlockCache = blockCache->IsBlockTypeActive(EBlockType::HashTableChunkIndex)
        ? blockCache
        : nullptr;

    return New<THashTableChunkIndexReadController>(
        chunkId,
        std::move(chunkMeta),
        columnFilter,
        std::move(keys),
        std::move(keyComparer),
        tableSchema,
        timestamp,
        produceAllVersions,
        std::move(actualBlockCache),
        std::move(testingOptions),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
