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
        const NLogging::TLogger& logger)
        : DeviceId_(deviceId)
        , DeviceConfig_(std::move(deviceConfig))
        , Client_(std::move(client))
        , Logger(logger)
    {
        // Setup name table.
        NameTable_->RegisterName("device_id");
        NameTable_->RegisterName("block_id");
        NameTable_->RegisterName("block_datum");

        // Save column ids.
        DeviceIdColumn_ = NameTable_->GetId("device_id");
        BlockIdColumn_ = NameTable_->GetId("block_id");
        BlockDatumColumn_ = NameTable_->GetId("block_datum");
    }

    std::map<i64, TString> ReadBlocks(
        std::map<i64, TString>&& blocks,
        bool fillEmptyBlocksWithZeros,
        const TGuid& readId)
    {
        YT_LOG_DEBUG("Start reading blocks (Blocks: %v, FillEmptyBlocksWithZeros: %v, ReadId: %v)",
            blocks.size(),
            fillEmptyBlocksWithZeros,
            readId);

        // First try to get blocks from the cache.
        std::map<i64, TString> blocksToRead;
        for (auto& [blockId, blockDatum] : blocks) {
            auto it = BlockCache_.find(blockId);
            if (it != BlockCache_.end()) {
                blockDatum = it->second;
            } else {
                blocksToRead.emplace(blockId, TString());
            }
        }

        if (!blocksToRead.empty()) {
            // Read blocks from the table.
            RowBuffer_->Clear();
            std::vector<TLegacyKey> keys;
            keys.reserve(blocks.size());
            for (const auto& [blockId, _] : blocksToRead) {
                TUnversionedRowBuilder builder;
                builder.AddValue(ToUnversionedValue(DeviceId_, RowBuffer_, DeviceIdColumn_));
                builder.AddValue(ToUnversionedValue(blockId, RowBuffer_, BlockIdColumn_));
                keys.push_back(RowBuffer_->CaptureRow(builder.GetRow()));
            }

            YT_LOG_DEBUG("Lookup (Blocks: %v, ReadId: %v)",
                keys.size(),
                readId);

            // TODO(yuryalekseev): Use select for contiguous blocks.
            auto rows = WaitFor(Client_->LookupRows(DeviceConfig_->TablePath, NameTable_, MakeSharedRange(std::move(keys), MakeStrong(this))))
                .ValueOrThrow()
                .Rowset->GetRows();

            YT_LOG_DEBUG("Lookuped (Blocks: %v, ReadId: %v)",
                rows.size(),
                readId);

            for (auto row : rows) {
                i64 deviceId, blockId;
                TString blockDatum;

                FromUnversionedValue(&deviceId, row[DeviceIdColumn_]);
                FromUnversionedValue(&blockId, row[BlockIdColumn_]);
                FromUnversionedValue(&blockDatum, row[BlockDatumColumn_]);

                YT_VERIFY(blocksToRead.contains(blockId));
                blocks[blockId] = blockDatum;
                BlockCache_[blockId] = std::move(blockDatum);
            }

            for (auto& [blockId, blockDatum] : blocks) {
                if (blockDatum.empty() && fillEmptyBlocksWithZeros) {
                    blockDatum = TString(DeviceConfig_->BlockSize, '\0');
                    BlockCache_[blockId] = blockDatum;
                }
            }
            RowBuffer_->Clear();
        }

        YT_LOG_DEBUG("Finished reading blocks (Blocks: %v, FillEmptyBlocksWithZeros: %v, ReadId: %v)",
            blocks.size(),
            fillEmptyBlocksWithZeros,
            readId);

        return std::move(blocks);
    }

    void WriteBlocks(
        std::map<i64, TString>&& blocks,
        bool flush,
        const TGuid& writeId)
    {
        YT_LOG_DEBUG("Start writing blocks (Blocks: %v, Flush: %v, WriteId: %v)",
            blocks.size(),
            flush,
            writeId);

        for (auto& [blockId, blockDatum] : blocks) {
            BlockCache_[blockId] = flush ? blockDatum : std::move(blockDatum);
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

        auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        YT_LOG_DEBUG("Started tablet transaction (TxId: %v, WriteId: %v)",
            tx->GetId(),
            writeId);

        RowBuffer_->Clear();
        std::vector<TUnversionedRow> rows;
        rows.reserve(blocks.size());
        for (auto& [blockId, blockDatum] : blocks) {
            TUnversionedRowBuilder builder;
            builder.AddValue(ToUnversionedValue(DeviceId_, RowBuffer_, DeviceIdColumn_));
            builder.AddValue(ToUnversionedValue(blockId, RowBuffer_, BlockIdColumn_));
            builder.AddValue(ToUnversionedValue(std::move(blockDatum), RowBuffer_, BlockDatumColumn_));
            rows.push_back(RowBuffer_->CaptureRow(builder.GetRow()));
        }

        tx->WriteRows(DeviceConfig_->TablePath, NameTable_, MakeSharedRange(std::move(rows), MakeStrong(this)));

        RowBuffer_->Clear();

        YT_LOG_DEBUG("Committing tablet transaction (TxId: %v, WriteId: %v)",
            tx->GetId(),
            writeId);

        WaitFor(tx->Commit())
            .ValueOrThrow();

        YT_LOG_DEBUG("Committed tablet transaction (WriteId: %v)",
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
            std::map<i64, TString> dirtyBlocks;
            for (auto blockId : DirtyBlocks_) {
                if (DeviceConfig_->WriteBatchSize <= std::ssize(dirtyBlocks)) {
                    break;
                }
                auto it = BlockCache_.find(blockId);
                YT_VERIFY(it != BlockCache_.end());
                dirtyBlocks.emplace(blockId, it->second);
            }

            WriteBlocks(std::move(dirtyBlocks), true /* flash */, TGuid::Create());
        }

        return VoidFuture;
    }

private:
    // TODO(yuryalekseev): Add support for max cache size.
    const i64 DeviceId_;
    const TDynamicTableBlockDeviceConfigPtr DeviceConfig_;
    const NApi::NNative::IClientPtr Client_;
    const NLogging::TLogger Logger;

    TNameTablePtr NameTable_ = New<TNameTable>();
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    int DeviceIdColumn_;
    int BlockIdColumn_;
    int BlockDatumColumn_;

    std::map<i64, TString> BlockCache_;
    std::unordered_set<i64> DirtyBlocks_;
};

DECLARE_REFCOUNTED_TYPE(TBlockCache)
DEFINE_REFCOUNTED_TYPE(TBlockCache)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableBlockDevice
    : public IBlockDevice
{
public:
    explicit TDynamicTableBlockDevice(
        const TString& deviceId,
        TDynamicTableBlockDeviceConfigPtr deviceConfig,
        NApi::NNative::IClientPtr client,
        const NLogging::TLogger& logger)
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
        auto blocks = BlockCache_->ReadBlocks(std::move(blockIds), true /* fillEmptyBlocksWithZeros */, readId);

        std::vector<TSharedRef> refs;
        refs.reserve(blocks.size());
        for (auto& [blockId, blockDatum] : blocks) {
            YT_VERIFY(0 < length);

            i64 beginWithinBlock = offset % DeviceConfig_->BlockSize;
            i64 endWithinBlock = std::min(beginWithinBlock + length, DeviceConfig_->BlockSize);
            i64 sizeWithinBlock = endWithinBlock - beginWithinBlock;

            YT_VERIFY(0 <= beginWithinBlock);
            YT_VERIFY(beginWithinBlock < endWithinBlock);
            YT_VERIFY(endWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= length);

            refs.insert(refs.end(), TSharedRef::FromString(blockDatum.substr(beginWithinBlock, sizeWithinBlock)));

            offset += sizeWithinBlock;
            length -= sizeWithinBlock;
        }
        YT_VERIFY(length == 0);

        // Merge refs into single ref.
        // TODO: Add support for std::move(refs) to MergeRefsToRef;
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
            THROW_ERROR_EXCEPTION("Table with NBD devices %Qlv doesn't exist",
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
        auto tableIsDynamic = attributes.Find<bool>("dynamic");
        if (!tableIsDynamic || !*tableIsDynamic) {
            THROW_ERROR_EXCEPTION("Table with NBD devices %Qlv is not dynamic",
                DeviceConfig_->TablePath);
        }
        auto tabletState = attributes.Find<TString>("tablet_state");
        if (!tabletState || *tabletState != "mounted") {
            THROW_ERROR_EXCEPTION("Table with NBD devices %Qlv is not mounted",
                DeviceConfig_->TablePath);
        }

        // Initialize device and block sizes.
        i64 deviceSize = -1, blockSize = -1;
        auto blocks = BlockCache_->ReadBlocks({{DeviceSizeBlockId, ""}, {BlockSizeBlockId, ""}}, false /* fillEmptyBlocksWithZeros */, TGuid::Create());
        for (auto& [blockId, blockDatum]: blocks) {
            YT_VERIFY(blockId == DeviceSizeBlockId || blockId == BlockSizeBlockId);

            if (blockDatum.empty()) {
                continue;
            }

            if (blockId == DeviceSizeBlockId) {
                deviceSize = std::stoll(blockDatum);
            }

            if (blockId == BlockSizeBlockId) {
                blockSize = std::stoll(blockDatum);
            }
        }

        YT_VERIFY((deviceSize == -1 && blockSize == -1) || (deviceSize != -1 && blockSize != -1));

        if (blockSize == -1) {
            YT_LOG_INFO("Save device and block sizes (DeviceSize: %v, BlockSize: %v, TablePath: %v)",
                DeviceConfig_->Size,
                DeviceConfig_->BlockSize,
                DeviceConfig_->TablePath);

            blocks[DeviceSizeBlockId] = ToString(DeviceConfig_->Size);
            blocks[BlockSizeBlockId] = ToString(DeviceConfig_->BlockSize);
            BlockCache_->WriteBlocks(std::move(blocks), true /* flush */, TGuid::Create());

            YT_LOG_INFO("Finish initialization of dynamic table block device (TablePath: %v)",
                DeviceConfig_->TablePath);

            return VoidFuture;
        }

        // Validate device and block sizes.
        if (deviceSize != DeviceConfig_->Size) {
            THROW_ERROR_EXCEPTION("Device size from config %Qlv doesn't match the actual device size %Qlv",
                DeviceConfig_->Size,
                deviceSize);
        }

        if (blockSize != DeviceConfig_->BlockSize) {
            THROW_ERROR_EXCEPTION("Block size from config %Qlv doesn't match the actual block size %Qlv",
                DeviceConfig_->BlockSize,
                blockSize);
        }

        YT_LOG_INFO("Finish initialization of dynamic table block device (TablePath: %v)",
            DeviceConfig_->TablePath);

        return VoidFuture;
    }

private:
    std::pair<i64, i64> CalcScopeBlockIds(i64 offset, i64 length) const
    {
        i64 beginBlockId = offset / DeviceConfig_->BlockSize;
        i64 endBlockId = (offset + length - 1) / DeviceConfig_->BlockSize + 1;
        return {beginBlockId, endBlockId};
    }

    std::map<i64, TString> CalcRangeBlockIds(i64 offset, i64 length) const
    {
        std::map<i64, TString> blockIds;
        auto [beginBlockId, endBlockId] = CalcScopeBlockIds(offset, length);
        for (i64 blockId = beginBlockId; blockId != endBlockId; ++blockId) {
            blockIds.emplace(blockId, TString());
        }

        return blockIds;
    }

    std::map<i64, TString> PrepareBlocksForWriting(std::map<i64, TString>&& blocks, const TSharedRef& data, i64 offset, const TGuid& writeId)
    {
        YT_LOG_DEBUG("Prepare blocks for writing (Blocks: %v, WriteId: %v)",
            blocks.size(),
            writeId);

        YT_VERIFY(!blocks.empty());
        YT_VERIFY(!data.empty());

        i64 length = data.size();

        const auto firstBlockId = offset / DeviceConfig_->BlockSize;
        YT_VERIFY(firstBlockId == blocks.begin()->first);
        const auto lastBlockId = (offset + length - 1) / DeviceConfig_->BlockSize;
        YT_VERIFY(lastBlockId == std::prev(blocks.end())->first);

        std::map<i64, TString> readBlockIds;
        if (offset % DeviceConfig_->BlockSize != 0) {
            // Read the first block
            readBlockIds.emplace(firstBlockId, TString());
        }

        if ((offset + length) % DeviceConfig_->BlockSize != 0) {
            // Read the last block
            readBlockIds.emplace(lastBlockId, TString());
        }

        // Read blocks that we are going to overwrite.
        if (!readBlockIds.empty()) {
            auto readBlocks = BlockCache_->ReadBlocks(std::move(readBlockIds), true /* fillEmptyBlocksWithZeros */, writeId);
            for (auto& [blockId, blockDatum] : readBlocks) {
                YT_VERIFY(blocks.contains(blockId));
                blocks[blockId] = std::move(blockDatum);
            }
        }

        i64 dataPos = 0;
        for (auto& [blockId, blockDatum] : blocks) {
            YT_VERIFY(0 < length);

            i64 beginWithinBlock = offset % DeviceConfig_->BlockSize;
            i64 endWithinBlock = std::min(beginWithinBlock + length, DeviceConfig_->BlockSize);
            i64 sizeWithinBlock = endWithinBlock - beginWithinBlock;

            YT_VERIFY(0 <= beginWithinBlock);
            YT_VERIFY(beginWithinBlock < endWithinBlock);
            YT_VERIFY(endWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= DeviceConfig_->BlockSize);
            YT_VERIFY(sizeWithinBlock <= length);
            YT_VERIFY(dataPos + sizeWithinBlock <= std::ssize(data));

            if (blockDatum.empty()) {
                blockDatum = TString(DeviceConfig_->BlockSize, '\0');
            }

            YT_VERIFY(std::ssize(blockDatum) == DeviceConfig_->BlockSize);

            /*
                Overwrite the middle of the block by new data.
                |---------------------------------|-----------------------------------------------|--------------------------------------|
                |  old data [0..beginWithinBlock) | new data [dataPos..dataPos + sizeWithinBlock) | old data [endWithinBlock..blockSize) |
                |---------------------------------|-----------------------------------------------|--------------------------------------|
                0                           beginWithinBlock                     beginWithinBlock + sizeWithinBlock                   blockSize
            */

            for (auto i = 0; i < sizeWithinBlock; ++i) {
                YT_VERIFY(beginWithinBlock + i < std::ssize(blockDatum));
                YT_VERIFY(dataPos + i < std::ssize(data));
                blockDatum[beginWithinBlock + i] = data[dataPos + i];
            }

            offset += sizeWithinBlock;
            dataPos += sizeWithinBlock;
            length -= sizeWithinBlock;
        }

        YT_VERIFY(length == 0);
        YT_VERIFY(dataPos == std::ssize(data));

        YT_LOG_DEBUG("Prepared blocks for writing (Blocks: %v, WriteId: %v)",
            blocks.size(),
            writeId);

        return std::move(blocks);
    }

private:
    static constexpr i64 DeviceSizeBlockId = -1;
    static constexpr i64 BlockSizeBlockId = -2;

    const i64 DeviceId_;
    const TDynamicTableBlockDeviceConfigPtr DeviceConfig_;
    const NApi::NNative::IClientPtr Client_;
    const NLogging::TLogger Logger;

    TBlockCachePtr BlockCache_;
};

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDynamicTableBlockDevice(
    const TString& deviceId,
    TDynamicTableBlockDeviceConfigPtr deviceConfig,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger)
{
    return New<TDynamicTableBlockDevice>(deviceId, std::move(deviceConfig), std::move(client), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
