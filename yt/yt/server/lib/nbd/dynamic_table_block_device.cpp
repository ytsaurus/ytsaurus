#include "dynamic_table_block_device.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>

#include <library/cpp/yt/logging/logger.h>

#include <util/digest/city.h>

namespace NYT::NNbd {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableBlockDeviceTag { };

////////////////////////////////////////////////////////////////////////////////

class TBlockCache
    : public virtual TRefCounted
{
public:
    TBlockCache(
        i64 deviceId,
        TDynamicTableBlockDeviceConfigPtr deviceConfig,
        NApi::NNative::IClientPtr client,
        NLogging::TLogger logger)
        : DeviceId_(deviceId)
        , DeviceConfig_(std::move(deviceConfig))
        , Client_(std::move(client))
        , Logger(std::move(logger))
    {
        // Setup name table.
        NameTable_->RegisterName("device_id");
        NameTable_->RegisterName("block_id");
        NameTable_->RegisterName("block_payload");

        // Save column ids.
        DeviceIdColumn_ = NameTable_->GetId("device_id");
        BlockIdColumn_ = NameTable_->GetId("block_id");
        BlockPayloadColumn_ = NameTable_->GetId("block_payload");
    }

    std::map<i64, TSharedMutableRef> ReadBlocks(
        std::map<i64, TSharedMutableRef>&& blocks,
        bool fillEmptyBlocksWithZeros,
        TGuid readId)
    {
        YT_LOG_DEBUG("Start reading blocks (Blocks: %v, FillEmptyBlocksWithZeros: %v, ReadId: %v)",
            blocks.size(),
            fillEmptyBlocksWithZeros,
            readId);

        // First try to get blocks from the cache.
        std::map<i64, TSharedMutableRef> blocksToRead;
        for (auto& [blockId, blockPayload] : blocks) {
            auto it = BlockCache_.find(blockId);
            if (it != BlockCache_.end()) {
                blockPayload = it->second;
            } else {
                blocksToRead.emplace(blockId, TSharedMutableRef::MakeEmpty());
            }
        }

        if (!blocksToRead.empty()) {
            // Read blocks from the table.
            TSharedRange<TUnversionedRow> rows;
            if (blocksToRead.begin()->first == blocksToRead.rbegin()->first + std::ssize(blocksToRead) - 1) {
                // The range is contiguous.
                rows = SelectRows(blocksToRead, readId);
            } else {
                rows = LookupRows(blocksToRead, readId);
            }

            for (auto row : rows) {
                auto deviceId = FromUnversionedValue<i64>(row[DeviceIdColumn_]);
                auto blockId = FromUnversionedValue<i64>(row[BlockIdColumn_]);
                auto blockPayload = TSharedMutableRef::MakeCopy<TDynamicTableBlockDeviceTag>(TSharedRef::FromString(FromUnversionedValue<TString>(row[BlockPayloadColumn_])));

                YT_VERIFY(deviceId == DeviceId_);
                YT_VERIFY(blocksToRead.contains(blockId));
                blocks[blockId] = blockPayload;
                BlockCache_[blockId] = blockPayload;
            }

            for (auto& [blockId, blockPayload] : blocks) {
                if (blockPayload.empty() && fillEmptyBlocksWithZeros) {
                    blockPayload = TSharedMutableRef::Allocate<TDynamicTableBlockDeviceTag>(DeviceConfig_->BlockSize, {.InitializeStorage = true});
                    BlockCache_[blockId] = blockPayload;
                }
            }
        }

        YT_LOG_DEBUG("Finish reading blocks (Blocks: %v, FillEmptyBlocksWithZeros: %v, ReadId: %v)",
            blocks.size(),
            fillEmptyBlocksWithZeros,
            readId);

        return std::move(blocks);
    }

    void WriteBlocks(
        std::map<i64, TSharedMutableRef>&& blocks,
        bool flush,
        TGuid writeId)
    {
        YT_LOG_DEBUG("Start writing blocks (Blocks: %v, Flush: %v, WriteId: %v)",
            blocks.size(),
            flush,
            writeId);

        for (auto& [blockId, blockPayload] : blocks) {
            BlockCache_[blockId] = blockPayload;
            DirtyBlocks_.insert(blockId);
        }

        if (!flush) {
            YT_LOG_DEBUG("Finish writing blocks (Blocks: %v, Flush: %v, WriteId: %v)",
                blocks.size(),
                flush,
                writeId);
            return;
        }

        YT_LOG_DEBUG("Starting tablet transaction (WriteId: %v)",
            writeId);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        YT_LOG_DEBUG("Started tablet transaction (TransactionId: %v, WriteId: %v)",
            transaction->GetId(),
            writeId);

        RowBuffer_->Clear();
        std::vector<TUnversionedRow> rows;
        rows.reserve(blocks.size());
        for (auto& [blockId, blockPayload] : blocks) {
            TUnversionedRowBuilder builder;
            builder.AddValue(ToUnversionedValue(DeviceId_, RowBuffer_, DeviceIdColumn_));
            builder.AddValue(ToUnversionedValue(blockId, RowBuffer_, BlockIdColumn_));
            builder.AddValue(ToUnversionedValue(ToString(blockPayload), RowBuffer_, BlockPayloadColumn_));
            rows.push_back(RowBuffer_->CaptureRow(builder.GetRow()));
        }

        transaction->WriteRows(DeviceConfig_->TablePath, NameTable_, MakeSharedRange(std::move(rows), MakeStrong(this)));

        RowBuffer_->Clear();

        YT_LOG_DEBUG("Start committing tablet transaction (TransactionId: %v, WriteId: %v)",
            transaction->GetId(),
            writeId);

        WaitFor(transaction->Commit())
            .ValueOrThrow();

        YT_LOG_DEBUG("Finish committing tablet transaction (WriteId: %v)",
            writeId);

        for (auto& [blockId, _] : blocks) {
            DirtyBlocks_.erase(blockId);
        }

        YT_LOG_DEBUG("Finish writing blocks (Blocks: %v, Flush: %v, WriteId: %v)",
            blocks.size(),
            flush,
            writeId);
    }

    TFuture<void> FlushDirtyBlocks()
    {
        while (!DirtyBlocks_.empty()) {
            std::map<i64, TSharedMutableRef> dirtyBlocks;
            for (auto blockId : DirtyBlocks_) {
                if (DeviceConfig_->WriteBatchSize <= std::ssize(dirtyBlocks)) {
                    break;
                }
                auto it = BlockCache_.find(blockId);
                YT_VERIFY(it != BlockCache_.end());
                dirtyBlocks.emplace(blockId, it->second);
            }

            WriteBlocks(std::move(dirtyBlocks), true /*flush*/, TGuid::Create());
        }

        return VoidFuture;
    }

private:
    // TODO(yuryalekseev): Add support for max cache size.
    const i64 DeviceId_;
    const TDynamicTableBlockDeviceConfigPtr DeviceConfig_;
    const NApi::NNative::IClientPtr Client_;
    const NLogging::TLogger Logger;

    const TNameTablePtr NameTable_ = New<TNameTable>();
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    int DeviceIdColumn_;
    int BlockIdColumn_;
    int BlockPayloadColumn_;

    std::map<i64, TSharedMutableRef> BlockCache_;
    std::unordered_set<i64> DirtyBlocks_;

private:
    TSharedRange<TUnversionedRow> SelectRows(const std::map<i64, TSharedMutableRef>& blocksToRead, TGuid readId)
    {
        YT_VERIFY(!blocksToRead.empty());
        YT_VERIFY(blocksToRead.begin()->first == blocksToRead.rbegin()->first + std::ssize(blocksToRead) - 1);

        YT_LOG_DEBUG("Start select (Blocks: %v, ReadId: %v)",
            blocksToRead.size(),
            readId);

        const auto query = Sprintf("* FROM [%s] WHERE device_id = %ld AND block_id >= %ld AND block_id <= %ld",
            DeviceConfig_->TablePath.c_str(),
            DeviceId_,
            blocksToRead.begin()->first,
            blocksToRead.rbegin()->first);

        auto rows = WaitFor(Client_->SelectRows(query))
            .ValueOrThrow()
            .Rowset->GetRows();

        YT_LOG_DEBUG("Finish select (Blocks: %v, ReadId: %v)",
            rows.size(),
            readId);

        return rows;
    }

    TSharedRange<TUnversionedRow> LookupRows(const std::map<i64, TSharedMutableRef>& blocksToRead, TGuid readId)
    {
        YT_VERIFY(!blocksToRead.empty());

        RowBuffer_->Clear();

        std::vector<TLegacyKey> keys;
        keys.reserve(blocksToRead.size());
        for (const auto& [blockId, _] : blocksToRead) {
            TUnversionedRowBuilder builder;
            builder.AddValue(ToUnversionedValue(DeviceId_, RowBuffer_, DeviceIdColumn_));
            builder.AddValue(ToUnversionedValue(blockId, RowBuffer_, BlockIdColumn_));
            keys.push_back(RowBuffer_->CaptureRow(builder.GetRow()));
        }

        YT_LOG_DEBUG("Start lookup (Blocks: %v, ReadId: %v)",
            keys.size(),
            readId);

        auto rows = WaitFor(Client_->LookupRows(DeviceConfig_->TablePath, NameTable_, MakeSharedRange(std::move(keys), MakeStrong(this))))
            .ValueOrThrow()
            .Rowset->GetRows();

        RowBuffer_->Clear();

        YT_LOG_DEBUG("Finish lookup (Blocks: %v, ReadId: %v)",
            rows.size(),
            readId);

        return rows;
    }
};

DECLARE_REFCOUNTED_TYPE(TBlockCache)
DEFINE_REFCOUNTED_TYPE(TBlockCache)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableBlockDevice
    : public IBlockDevice
{
public:
    explicit TDynamicTableBlockDevice(
        TString deviceId,
        TDynamicTableBlockDeviceConfigPtr deviceConfig,
        NApi::NNative::IClientPtr client,
        NLogging::TLogger logger)
        : DeviceId_(CityHash64(deviceId.data(), deviceId.size()))
        , DeviceConfig_(std::move(deviceConfig))
        , Client_(std::move(client))
        , Logger(logger.WithTag("DeviceId: %v", deviceId))
        , BlockCache_(New<TBlockCache>(DeviceId_, DeviceConfig_, Client_, Logger))
    { }

    virtual i64 GetTotalSize() const override
    {
        return DeviceConfig_->Size;
    }

    virtual bool IsReadOnly() const override
    {
        return false;
    }

    virtual TString DebugString() const override
    {
        return TString();
    }

    virtual TString GetProfileSensorTag() const override
    {
        return TString();
    }

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        auto readId = TGuid::Create();

        YT_LOG_DEBUG("Start read (Offset: %v, Length: %v, ReadId: %v)",
            offset,
            length,
            readId);

        if (length == 0) {
            YT_LOG_DEBUG("Finish read (Offset: %v, Length: %v, ReadId: %v)",
                offset,
                length,
                readId);
            return MakeFuture<TSharedRef>({});
        }

        auto blockIds = CalcRangeBlockIds(offset, length);
        auto blocks = BlockCache_->ReadBlocks(std::move(blockIds), true /*fillEmptyBlocksWithZeros*/, readId);

        std::vector<TSharedRef> refs;
        refs.reserve(blocks.size());
        for (auto& [blockId, blockPayload] : blocks) {
            YT_VERIFY(0 < length);

            i64 beginWithinBlock = offset % DeviceConfig_->BlockSize;
            i64 endWithinBlock = std::min(beginWithinBlock + length, DeviceConfig_->BlockSize);
            i64 sizeWithinBlock = endWithinBlock - beginWithinBlock;

            YT_VERIFY(0 <= beginWithinBlock);
            YT_VERIFY(beginWithinBlock < endWithinBlock);
            YT_VERIFY(endWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= length);

            refs.insert(refs.end(), blockPayload.Slice(beginWithinBlock, endWithinBlock));

            offset += sizeWithinBlock;
            length -= sizeWithinBlock;
        }
        YT_VERIFY(length == 0);

        // Merge refs into single ref.
        auto result = MergeRefsToRef<TDynamicTableBlockDeviceTag>(refs);

        YT_LOG_DEBUG("Finish read (Size: %v, ReadId: %v)",
            result.size(),
            readId);

        return MakeFuture<TSharedRef>(std::move(result));
    }

    virtual TFuture<void> Write(
        i64 offset,
        const TSharedRef& data,
        const TWriteOptions& options) override
    {
        i64 length = data.size();
        auto writeId = TGuid::Create();

        YT_LOG_DEBUG("Start write (Offset: %v, Length: %v, Flush: %v, WriteId: %v)",
            offset,
            length,
            options.Flush,
            writeId);

        if (length == 0) {
            YT_LOG_DEBUG("Finish write (Offset: %v, Length: %v, Flush: %v, WriteId: %v)",
                offset,
                length,
                options.Flush,
                writeId);

            return VoidFuture;
        }

        auto blockIds = CalcRangeBlockIds(offset, length);
        auto blocks = PrepareBlocksForWriting(std::move(blockIds), data, offset, writeId);
        BlockCache_->WriteBlocks(std::move(blocks), options.Flush, writeId);

        YT_LOG_DEBUG("Finish write (Offset: %v, Length: %v, Flush: %v, WriteId: %v)",
            offset,
            length,
            options.Flush,
            writeId);

        return VoidFuture;
    }

    TFuture<void> Flush() override
    {
        YT_LOG_INFO("Start flush (TablePath: %v)",
            DeviceConfig_->TablePath);

        auto future = BlockCache_->FlushDirtyBlocks();
        WaitFor(future).ThrowOnError();

        YT_LOG_INFO("Finish flush (TablePath: %v)",
            DeviceConfig_->TablePath);

        return VoidFuture;
    }

    TFuture<void> Initialize() override
    {
        YT_LOG_INFO("Start initialization of dynamic table block device (TablePath: %v)",
            DeviceConfig_->TablePath);

        // Check that the table does exist.
        auto tableExists = WaitFor(Client_->NodeExists(DeviceConfig_->TablePath))
            .ValueOrThrow();

        if (!tableExists) {
            THROW_ERROR_EXCEPTION("Table with NBD devices %v does not exist",
                DeviceConfig_->TablePath);
        }

        // Check that the table is indeed dynamic and is mounted.
        NApi::TGetNodeOptions options{
            .Attributes = std::vector<TString>{"dynamic", "tablet_state"}
        };
        auto rsp = WaitFor(Client_->GetNode(DeviceConfig_->TablePath, options))
            .ValueOrThrow();
        auto rspNode = NYTree::ConvertTo<NYTree::INodePtr>(rsp);
        const auto& attributes = rspNode->Attributes();
        auto tableIsDynamic = attributes.Find<bool>("dynamic").value_or(false);
        if (!tableIsDynamic) {
            THROW_ERROR_EXCEPTION("Table with NBD devices %v is not dynamic",
                DeviceConfig_->TablePath);
        }
        auto tabletState = attributes.Find<ETabletState>("tablet_state").value_or(ETabletState::Unmounted);
        if (tabletState != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Table with NBD devices %v is not mounted",
                DeviceConfig_->TablePath);
        }

        // Initialize device and block sizes.
        i64 deviceSize = -1, blockSize = -1;
        auto blocks = BlockCache_->ReadBlocks({{DeviceSizeBlockId, {}}, {BlockSizeBlockId, {}}}, false /*fillEmptyBlocksWithZeros*/, TGuid::Create());
        for (auto& [blockId, blockPayload]: blocks) {
            YT_VERIFY(blockId == DeviceSizeBlockId || blockId == BlockSizeBlockId);

            if (blockPayload.empty()) {
                continue;
            }

            if (blockId == DeviceSizeBlockId) {
                deviceSize = std::stoll(ToString(blockPayload));
            }

            if (blockId == BlockSizeBlockId) {
                blockSize = std::stoll(ToString(blockPayload));
            }
        }

        YT_VERIFY((deviceSize == -1 && blockSize == -1) || (deviceSize != -1 && blockSize != -1));

        if (blockSize == -1) {
            YT_LOG_INFO("Save device and block sizes (DeviceSize: %v, BlockSize: %v, TablePath: %v)",
                DeviceConfig_->Size,
                DeviceConfig_->BlockSize,
                DeviceConfig_->TablePath);

            blocks[DeviceSizeBlockId] = TSharedMutableRef::MakeCopy<TDynamicTableBlockDeviceTag>(TSharedRef::FromString(ToString(DeviceConfig_->Size)));
            blocks[BlockSizeBlockId] = TSharedMutableRef::MakeCopy<TDynamicTableBlockDeviceTag>(TSharedRef::FromString(ToString(DeviceConfig_->BlockSize)));
            BlockCache_->WriteBlocks(std::move(blocks), true /*flush*/, TGuid::Create());

            YT_LOG_INFO("Finish initialization of dynamic table block device (TablePath: %v)",
                DeviceConfig_->TablePath);

            return VoidFuture;
        }

        // Validate device and block sizes.
        if (deviceSize != DeviceConfig_->Size) {
            THROW_ERROR_EXCEPTION("Device size from config %Ql does not match the actual device size %Ql of device %Qv",
                DeviceConfig_->Size,
                deviceSize,
                DeviceId_);
        }

        if (blockSize != DeviceConfig_->BlockSize) {
            THROW_ERROR_EXCEPTION("Block size from config %Ql does not match the actual block size %Ql of device %Qv",
                DeviceConfig_->BlockSize,
                blockSize,
                DeviceId_);
        }

        YT_LOG_INFO("Finish initialization of dynamic table block device (TablePath: %v)",
            DeviceConfig_->TablePath);

        return VoidFuture;
    }

private:
    static constexpr i64 DeviceSizeBlockId = -1;
    static constexpr i64 BlockSizeBlockId = -2;

    const i64 DeviceId_;
    const TDynamicTableBlockDeviceConfigPtr DeviceConfig_;
    const NApi::NNative::IClientPtr Client_;
    const NLogging::TLogger Logger;

    TBlockCachePtr BlockCache_;

private:
    std::pair<i64, i64> CalcScopeBlockIds(i64 offset, i64 length) const
    {
        i64 beginBlockId = offset / DeviceConfig_->BlockSize;
        i64 endBlockId = (offset + length - 1) / DeviceConfig_->BlockSize + 1;
        return {beginBlockId, endBlockId};
    }

    std::map<i64, TSharedMutableRef> CalcRangeBlockIds(i64 offset, i64 length) const
    {
        std::map<i64, TSharedMutableRef> blockIds;
        auto [beginBlockId, endBlockId] = CalcScopeBlockIds(offset, length);
        for (i64 blockId = beginBlockId; blockId != endBlockId; ++blockId) {
            blockIds.emplace(blockId, TSharedMutableRef::MakeEmpty());
        }

        return blockIds;
    }

    std::map<i64, TSharedMutableRef> PrepareBlocksForWriting(std::map<i64, TSharedMutableRef>&& blocks, const TSharedRef& data, i64 offset, TGuid writeId)
    {
        YT_LOG_DEBUG("Start preparing blocks for writing (Blocks: %v, WriteId: %v)",
            blocks.size(),
            writeId);

        YT_VERIFY(!blocks.empty());
        YT_VERIFY(!data.empty());

        i64 length = data.size();

        const auto firstBlockId = offset / DeviceConfig_->BlockSize;
        YT_VERIFY(firstBlockId == blocks.begin()->first);
        const auto lastBlockId = (offset + length - 1) / DeviceConfig_->BlockSize;
        YT_VERIFY(lastBlockId == std::prev(blocks.end())->first);

        std::map<i64, TSharedMutableRef> readBlockIds;
        if (offset % DeviceConfig_->BlockSize != 0) {
            // Read the first block
            readBlockIds.emplace(firstBlockId, TSharedMutableRef::MakeEmpty());
        }

        if ((offset + length) % DeviceConfig_->BlockSize != 0) {
            // Read the last block
            readBlockIds.emplace(lastBlockId, TSharedMutableRef::MakeEmpty());
        }

        // Read blocks that we are going to overwrite.
        if (!readBlockIds.empty()) {
            auto readBlocks = BlockCache_->ReadBlocks(std::move(readBlockIds), true /*fillEmptyBlocksWithZeros*/, writeId);
            for (auto& [blockId, blockPayload] : readBlocks) {
                YT_VERIFY(blocks.contains(blockId));
                blocks[blockId] = blockPayload;
            }
        }

        i64 dataOffset = 0;
        for (auto& [blockId, blockPayload] : blocks) {
            YT_VERIFY(0 < length);

            i64 beginWithinBlock = offset % DeviceConfig_->BlockSize;
            i64 endWithinBlock = std::min(beginWithinBlock + length, DeviceConfig_->BlockSize);
            i64 sizeWithinBlock = endWithinBlock - beginWithinBlock;

            YT_VERIFY(0 <= beginWithinBlock);
            YT_VERIFY(beginWithinBlock < endWithinBlock);
            YT_VERIFY(endWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= length);
            YT_VERIFY(dataOffset + sizeWithinBlock <= std::ssize(data));

            if (blockPayload.empty()) {
                blockPayload = TSharedMutableRef::Allocate<TDynamicTableBlockDeviceTag>(DeviceConfig_->BlockSize, {.InitializeStorage = true});
            }

            YT_VERIFY(std::ssize(blockPayload) == DeviceConfig_->BlockSize);

            /*
                Overwrite the middle of the block by new data.
                |---------------------------------|-----------------------------------------------------|--------------------------------------|
                |  old data [0..beginWithinBlock) | new data [dataOffset..dataOffset + sizeWithinBlock) | old data [endWithinBlock..blockSize) |
                |---------------------------------|-----------------------------------------------------|--------------------------------------|
                0                           beginWithinBlock                           beginWithinBlock + sizeWithinBlock                   blockSize
            */

            for (auto i = 0; i < sizeWithinBlock; ++i) {
                YT_VERIFY(beginWithinBlock + i < std::ssize(blockPayload));
                YT_VERIFY(dataOffset + i < std::ssize(data));
                blockPayload[beginWithinBlock + i] = data[dataOffset + i];
            }

            offset += sizeWithinBlock;
            dataOffset += sizeWithinBlock;
            length -= sizeWithinBlock;
        }

        YT_VERIFY(length == 0);
        YT_VERIFY(dataOffset == std::ssize(data));

        YT_LOG_DEBUG("Finish preparing blocks for writing (Blocks: %v, WriteId: %v)",
            blocks.size(),
            writeId);

        return std::move(blocks);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDynamicTableBlockDevice(
    TString deviceId,
    TDynamicTableBlockDeviceConfigPtr deviceConfig,
    NApi::NNative::IClientPtr client,
    NLogging::TLogger logger)
{
    return New<TDynamicTableBlockDevice>(
        std::move(deviceId),
        std::move(deviceConfig),
        std::move(client),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
