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

namespace NYT::NNbd {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableBlockDeviceTag { };

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
        : DeviceId_(deviceId)
        , DeviceConfig_(std::move(deviceConfig))
        , Client_(std::move(client))
        , Logger(logger.WithTag("DeviceId: %v", deviceId))
    {}

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

        auto blockIds = CalcRangeBlockIds(offset, length, readId);
        auto blocks = ReadBlocks(std::move(blockIds), readId);

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

            YT_LOG_DEBUG("Reading (Offset: %v, Length: %v, BlockSize: %v, ReadId: %v)",
                beginWithinBlock,
                sizeWithinBlock,
                blockDatum.size(),
                readId);

            refs.insert(refs.end(), TSharedRef::FromString(blockDatum.substr(beginWithinBlock, sizeWithinBlock)));

            offset += sizeWithinBlock;
            length -= sizeWithinBlock;
        }
        YT_VERIFY(length == 0);

        // Merge refs into single ref.
        // TODO: std::move(refs);
        auto result = MergeRefsToRef<TDynamicTableBlockDeviceTag>(refs);

        YT_LOG_DEBUG("Finish read (Size: %v, ReadId: %v)",
            result.size(),
            readId);

        return MakeFuture<TSharedRef>(std::move(result));
    }

    virtual TFuture<void> Write(
        i64 offset,
        const TSharedRef& data,
        const TWriteOptions& /*options*/) override
    {
        i64 length = data.size();
        auto writeId = TGuid::Create();

        YT_LOG_DEBUG("Start write (Offset: %v, Length: %v, WriteId: %v)",
            offset,
            length,
            writeId);

        if (length == 0) {
            YT_LOG_DEBUG("Finish write (Offset: %v, Length: %v, WriteId: %v)",
                offset,
                length,
                writeId);

            return VoidFuture;
        }

        auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        auto blockIds = CalcRangeBlockIds(offset, length, writeId);
        auto blocks = PrepareBlocksForWriting(std::move(blockIds), data, offset, writeId);
        WriteBlocks(std::move(blocks), tx, writeId);

        WaitFor(tx->Commit())
            .ThrowOnError();

        YT_LOG_DEBUG("Finish write (Offset: %v, Length: %v, WriteId: %v)",
            offset,
            length,
            writeId);

        return VoidFuture;
    }

    TFuture<void> Flush() override
    {
        return VoidFuture;
    }

    TFuture<void> Initialize() override
    {
        YT_LOG_DEBUG("Start initialization");

        auto tableExists = WaitFor(Client_->NodeExists(DeviceConfig_->TablePath))
            .ValueOrThrow();

        if (!tableExists) {
            THROW_ERROR_EXCEPTION("Table with NBD devices %Qlv doesn't exist", DeviceConfig_->TablePath);
        }

        // Setup name table.
        NameTable_->RegisterName("device_id");
        NameTable_->RegisterName("block_id");
        NameTable_->RegisterName("block_datum");

        // Save column ids.
        DeviceIdColumn_ = NameTable_->GetId("device_id");
        BlockIdColumn_ = NameTable_->GetId("block_id");
        BlockDatumColumn_ = NameTable_->GetId("block_datum");

        // Initialize device and block sizes.
        static constexpr i64 DeviceSizeBlockId = -1;
        static constexpr i64 BlockSizeBlockId = -2;

        RowBuffer_->Clear();
        std::vector<TLegacyKey> keys;
        keys.reserve(2);
        for (auto blockId : {DeviceSizeBlockId, BlockSizeBlockId})
        {
            TUnversionedRowBuilder builder;
            builder.AddValue(ToUnversionedValue(DeviceId_, RowBuffer_, DeviceIdColumn_));
            builder.AddValue(ToUnversionedValue(blockId, RowBuffer_, BlockIdColumn_));
            keys.push_back(RowBuffer_->CaptureRow(builder.GetRow()));
        }
        RowBuffer_->Clear();

        auto rowset = WaitFor(Client_->LookupRows(DeviceConfig_->TablePath, NameTable_, MakeSharedRange(std::move(keys), MakeStrong(this))))
            .ValueOrThrow()
            .Rowset;

        i64 deviceSize = -1, blockSize = -1;
        for (auto row : rowset->GetRows()) {
            i64 blockId;
            TString blockDatum;
            FromUnversionedValue(&blockId, row[BlockIdColumn_]);
            FromUnversionedValue(&blockDatum, row[BlockDatumColumn_]);

            YT_VERIFY(blockId == DeviceSizeBlockId || blockId == BlockSizeBlockId);

            if (!blockDatum.empty()) {
                if (blockId == DeviceSizeBlockId) {
                    deviceSize = std::stoll(blockDatum);
                }

                if (blockId == BlockSizeBlockId) {
                    blockSize = std::stoll(blockDatum);
                }
            }
        }

        YT_VERIFY((deviceSize == -1 && blockSize == -1) || (deviceSize != -1 && blockSize != -1));

        if (blockSize == -1) {
            YT_LOG_DEBUG("Saving device and block sizes into table (DeviceSize: %v, BlockSize: %v, TablePath: %v)",
                DeviceConfig_->Size,
                DeviceConfig_->BlockSize,
                DeviceConfig_->TablePath);

            std::map<i64, TString> blocks;
            blocks[DeviceSizeBlockId] = ToString(DeviceConfig_->Size);
            blocks[BlockSizeBlockId] = ToString(DeviceConfig_->BlockSize);

            auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();

            WriteBlocks(std::move(blocks), tx, TGuid::Create());

            WaitFor(tx->Commit())
                .ValueOrThrow();

            return VoidFuture;
        }

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

        return VoidFuture;
    }

private:
    // TODO(yuryalekseev): read by batch.
    std::map<i64, TString> ReadBlocks(
        std::map<i64, TString>&& blocks,
        const TGuid& readId)
    {
        YT_LOG_DEBUG("Start reading blocks (Blocks: %v, ReadId: %v)",
            blocks.size(),
            readId);

        RowBuffer_->Clear();
        std::vector<TLegacyKey> keys;
        keys.reserve(blocks.size());
        for (const auto& [blockId, _] : blocks) {
            TUnversionedRowBuilder builder;
            builder.AddValue(ToUnversionedValue(DeviceId_, RowBuffer_, DeviceIdColumn_));
            builder.AddValue(ToUnversionedValue(blockId, RowBuffer_, BlockIdColumn_));
            keys.push_back(RowBuffer_->CaptureRow(builder.GetRow()));
        }

        auto rowset = WaitFor(Client_->LookupRows(DeviceConfig_->TablePath, NameTable_, MakeSharedRange(std::move(keys), MakeStrong(this))))
            .ValueOrThrow()
            .Rowset;

        for (auto row : rowset->GetRows()) {
            i64 blockId;
            TString blockDatum;
            FromUnversionedValue(&blockId, row[BlockIdColumn_]);
            FromUnversionedValue(&blockDatum, row[BlockDatumColumn_]);

            YT_VERIFY(blocks.contains(blockId));
            blocks[blockId] = std::move(blockDatum);
        }

        for (auto& [_, blockDatum] : blocks) {
            if (blockDatum.empty()) {
                blockDatum = TString(DeviceConfig_->BlockSize, '\0');
            }
        }

        RowBuffer_->Clear();

        YT_LOG_DEBUG("Finished reading blocks (Blocks: %v, ReadId: %v)",
            blocks.size(),
            readId);

        return std::move(blocks);
    }

    // TODO(yuryalekseev): write by batch.
    void WriteBlocks(
        std::map<i64, TString>&& blocks,
        const NApi::ITransactionPtr& tx,
        const TGuid& writeId)
    {
        YT_LOG_DEBUG("Start writing blocks (Blocks: %v, WriteId: %v)",
            blocks.size(),
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

        YT_LOG_DEBUG("Finished writing blocks (Blocks: %v, WriteId: %v)",
            blocks.size(),
            writeId);
    }

    std::pair<i64, i64> CalcScopeBlockIds(i64 offset, i64 length) const
    {
        i64 beginBlockId = offset / DeviceConfig_->BlockSize;
        i64 endBlockId = (offset + length - 1) / DeviceConfig_->BlockSize + 1;
        return {beginBlockId, endBlockId};
    }

    std::map<i64, TString> CalcRangeBlockIds(i64 offset, i64 length, const TGuid& writeId) const
    {
        std::map<i64, TString> blockIds;
        auto [beginBlockId, endBlockId] = CalcScopeBlockIds(offset, length);
        for (i64 blockId = beginBlockId; blockId != endBlockId; ++blockId) {
            blockIds.emplace(blockId, TString());
        }

        YT_LOG_DEBUG("Calculated range of block ids (Offset: %v, Length: %v, BlockIds: %v, WriteId: %v)",
            offset,
            length,
            blockIds.size(),
            writeId);

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
        auto readBlocks = ReadBlocks(std::move(readBlockIds), writeId);
        for (auto& [blockId, blockDatum] : readBlocks) {
            YT_VERIFY(blocks.contains(blockId));
            blocks[blockId] = std::move(blockDatum);
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

            YT_LOG_DEBUG("Prepare block for writing (DataPos: %v, SizeWithinBlock: %v, BeginWithinBlock: %v, EndWithinBlock: %v, WriteId: %v)",
                dataPos,
                sizeWithinBlock,
                beginWithinBlock,
                endWithinBlock,
                writeId);

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
    const TString DeviceId_;
    const TDynamicTableBlockDeviceConfigPtr DeviceConfig_;
    const NApi::NNative::IClientPtr Client_;
    const NLogging::TLogger Logger;

    TNameTablePtr NameTable_ = New<TNameTable>();
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    int DeviceIdColumn_;
    int BlockIdColumn_;
    int BlockDatumColumn_;
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
