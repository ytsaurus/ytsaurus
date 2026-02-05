#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <library/cpp/yt/misc/range_helpers.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;

using namespace testing;

using NChunkClient::NProto::TMiscExt;
using NTableClient::TTableWriterOptions;
using NTableClient::TTableWriterConfig;
using NTableClient::TTableReaderOptions;

////////////////////////////////////////////////////////////////////////////////

class TReaderInterruptions
    : public TApiTestBase
{
protected:
    struct TExpectedSliceDescriptor
    {
        int RowCount = 0;
        int LowerRowIndex = 0;
        int UpperRowIndex = 0;
        i64 DataWeight = 0;
        i64 CompressedDataSize = 0;
        i64 UncompressedDataSize = 0;
        bool HasLowerKey = false;
    };

    NNative::IClientPtr NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
    NNative::IConnectionPtr NativeConnection_ = NativeClient_->GetNativeConnection();
    TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector{TColumnSchema("partition", ESimpleLogicalValueType::Int64)});
    ITransactionPtr Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, {}))
        .ValueOrThrow();
    TNameTablePtr NameTable_ = TNameTable::FromSchema(*Schema_);

    NChunkClient::NProto::TChunkSpec WriteBatchOfRows(std::vector<int> batch)
    {
        auto options = New<TTableWriterOptions>();
        options->Account = "tmp";

        auto writer = CreateSchemalessMultiChunkWriter(
            New<TTableWriterConfig>(),
            options,
            NameTable_,
            Schema_,
            /*lastKey*/ {},
            NativeClient_,
            /*localHostName*/ {},
            CellTagFromId(Transaction_->GetId()),
            Transaction_->GetId(),
            NullTableSchemaId,
            /*dataSink*/ {},
            /*writeBlocksOptions*/ {});

        auto makeRow = [&] (i64 partition) {
            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(partition, NameTable_->GetId("partition")));
            return builder.FinishRow();
        };

        auto owningRows = TransformRangeTo<std::vector<TUnversionedOwningRow>>(batch, makeRow);
        auto rows = TransformRangeTo<std::vector<TUnversionedRow>>(owningRows, &TUnversionedOwningRow::Get);
        YT_VERIFY(writer->Write(TRange(rows)));

        WaitFor(writer->Close())
            .ThrowOnError();

        auto chunkSpecs = writer->GetWrittenChunkSpecs();
        YT_VERIFY(ssize(chunkSpecs) == 1);
        chunkSpecs[0].set_use_new_override_semantics(true);
        return std::move(chunkSpecs[0]);
    }

    TMiscExt FetchMiscExt(TChunkId chunkId)
    {
        auto chunkReplica = ConvertTo<std::string>(WaitFor(Client_->GetNode(Format("#%v/@stored_replicas/0", chunkId)))
            .ValueOrThrow());

        TDataNodeServiceProxy proxy(NativeConnection_->CreateChannelByAddress(chunkReplica));

        auto req = proxy.GetChunkMeta();
        ToProto(req->mutable_chunk_id(), chunkId);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        return GetProtoExtension<TMiscExt>(rsp->chunk_meta().extensions());
    }

    void VerifyInterruptDescriptor(
        const TInterruptDescriptor& descriptor,
        const std::optional<TExpectedSliceDescriptor>& expectedRead,
        const std::optional<TExpectedSliceDescriptor>& expectedUnread)
    {
        auto verifyDescriptors = [] (
            const auto& descriptors,
            const std::optional<TExpectedSliceDescriptor>& expected,
            bool isRead)
        {
            if (!expected) {
                EXPECT_THAT(descriptors, IsEmpty());
                return;
            }

            EXPECT_THAT(descriptors, Not(IsEmpty()));

            // Calculate totals from all descriptors.
            int totalRows = 0;
            i64 totalDataWeight = 0;
            i64 totalCompressedSize = 0;
            i64 totalUncompressedSize = 0;
            bool hasLowerKey = false;

            for (const auto& sliceDescriptor : descriptors) {
                const auto& chunkSpec = sliceDescriptor.GetSingleChunk();
                totalRows += chunkSpec.row_count_override();
                totalDataWeight += chunkSpec.data_weight_override();
                totalCompressedSize += chunkSpec.compressed_data_size_override();
                totalUncompressedSize += chunkSpec.uncompressed_data_size_override();

                if (chunkSpec.lower_limit().has_legacy_key() &&
                    chunkSpec.lower_limit().has_key_bound_prefix()) {
                    hasLowerKey = true;
                    EXPECT_TRUE(chunkSpec.lower_limit().key_bound_is_inclusive());
                }
            }

            if (expected->RowCount > 0) {
                EXPECT_EQ(totalRows, expected->RowCount);
            }
            if (expected->DataWeight > 0) {
                if (!isRead) {
                    EXPECT_GE(totalDataWeight, 1);
                }
                EXPECT_NEAR(totalDataWeight, expected->DataWeight, 1.0);
            }
            if (expected->CompressedDataSize > 0) {
                if (!isRead) {
                    EXPECT_GE(totalCompressedSize, 1);
                }
                EXPECT_NEAR(totalCompressedSize, expected->CompressedDataSize, 1.0);
            }
            if (expected->UncompressedDataSize > 0) {
                if (!isRead) {
                    EXPECT_GE(totalUncompressedSize, 1);
                }
                EXPECT_NEAR(totalUncompressedSize, expected->UncompressedDataSize, 1.0);
            }
            if (expected->HasLowerKey) {
                EXPECT_TRUE(hasLowerKey);
            }
        };

        verifyDescriptors(descriptor.ReadDataSliceDescriptors, expectedRead, /*isRead*/ true);
        verifyDescriptors(descriptor.UnreadDataSliceDescriptors, expectedUnread, /*isRead*/ false);
    }
};

TEST_F(TReaderInterruptions, MultipleChunksNoOverrides)
{
    auto chunkSpec1 = WriteBatchOfRows({1, 2});
    auto chunkSpec2 = WriteBatchOfRows({3, 4, 5});
    auto chunkSpec3 = WriteBatchOfRows({6, 7, 8, 9});

    auto miscExt1 = FetchMiscExt(FromProto<TChunkId>(chunkSpec1.chunk_id()));
    auto miscExt2 = FetchMiscExt(FromProto<TChunkId>(chunkSpec2.chunk_id()));
    auto miscExt3 = FetchMiscExt(FromProto<TChunkId>(chunkSpec3.chunk_id()));

    chunkSpec1.set_row_count_override(miscExt1.row_count());
    chunkSpec2.set_row_count_override(miscExt2.row_count());
    chunkSpec3.set_row_count_override(miscExt3.row_count());

    int totalRows = miscExt1.row_count() + miscExt2.row_count() + miscExt3.row_count();
    i64 totalDataWeight = miscExt1.data_weight() + miscExt2.data_weight() + miscExt3.data_weight();
    i64 totalCompressedSize = miscExt1.compressed_data_size() + miscExt2.compressed_data_size() + miscExt3.compressed_data_size();
    i64 totalUncompressedSize = miscExt1.uncompressed_data_size() + miscExt2.uncompressed_data_size() + miscExt3.uncompressed_data_size();

    auto sourceDirectory = New<TDataSourceDirectory>();
    auto& source = sourceDirectory->DataSources().emplace_back(New<TDataSource>());
    source->Schema() = Schema_;

    std::vector<TDataSliceDescriptor> dataSlices = {
        TDataSliceDescriptor(chunkSpec1),
        TDataSliceDescriptor(chunkSpec2),
        TDataSliceDescriptor(chunkSpec3)
    };

    for (bool isParallel : {false, true}) {
        auto chunkReaderHost = TChunkReaderHost::FromClient(NativeClient_);
        auto multiChunkReaderHost = CreateSingleSourceMultiChunkReaderHost(chunkReaderHost);

        ISchemalessMultiChunkReaderPtr reader = isParallel
            ? CreateSchemalessParallelMultiReader(
                New<TTableReaderConfig>(),
                New<TTableReaderOptions>(),
                multiChunkReaderHost,
                sourceDirectory,
                dataSlices,
                /*hintKeyPrefixes*/ std::nullopt,
                NameTable_,
                /*chunkReadOptions*/ {},
                TReaderInterruptionOptions::InterruptibleWithEmptyKey())
            : CreateSchemalessSequentialMultiReader(
                New<TTableReaderConfig>(),
                New<TTableReaderOptions>(),
                multiChunkReaderHost,
                sourceDirectory,
                dataSlices,
                /*hintKeyPrefixes*/ std::nullopt,
                NameTable_,
                /*chunkReadOptions*/ {},
                TReaderInterruptionOptions::InterruptibleWithEmptyKey());

        WaitFor(reader->GetReadyEvent())
            .ThrowOnError();

        auto initialDescriptor = reader->GetInterruptDescriptor({});
        EXPECT_THAT(initialDescriptor.ReadDataSliceDescriptors, IsEmpty());
        EXPECT_THAT(initialDescriptor.UnreadDataSliceDescriptors, Not(IsEmpty()));

        int totalUnreadRows = 0;
        for (const auto& unreadDesc : initialDescriptor.UnreadDataSliceDescriptors) {
            totalUnreadRows += unreadDesc.GetSingleChunk().row_count_override();
        }
        EXPECT_EQ(totalUnreadRows, totalRows);

        int rowsRead = 0;

        while (auto batch = reader->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            auto rows = batch->MaterializeRows();
            rowsRead += rows.size();

            auto descriptor = reader->GetInterruptDescriptor({});

            int readRows = 0;
            for (const auto& readDesc : descriptor.ReadDataSliceDescriptors) {
                readRows += readDesc.GetSingleChunk().row_count_override();
            }

            int unreadRows = 0;
            for (const auto& unreadDesc : descriptor.UnreadDataSliceDescriptors) {
                unreadRows += unreadDesc.GetSingleChunk().row_count_override();
            }

            EXPECT_EQ(readRows + unreadRows, totalRows);

            EXPECT_EQ(readRows, rowsRead);
        }

        EXPECT_EQ(rowsRead, totalRows);

        auto finalDescriptor = reader->GetInterruptDescriptor({});
        EXPECT_THAT(finalDescriptor.UnreadDataSliceDescriptors, IsEmpty());

        int finalReadRows = 0;
        i64 finalDataWeight = 0;
        i64 finalCompressedSize = 0;
        i64 finalUncompressedSize = 0;

        for (const auto& readDesc : finalDescriptor.ReadDataSliceDescriptors) {
            const auto& chunkSpec = readDesc.GetSingleChunk();
            finalReadRows += chunkSpec.row_count_override();
            finalDataWeight += chunkSpec.data_weight_override();
            finalCompressedSize += chunkSpec.compressed_data_size_override();
            finalUncompressedSize += chunkSpec.uncompressed_data_size_override();
        }

        EXPECT_EQ(finalReadRows, totalRows);
        EXPECT_EQ(finalDataWeight, totalDataWeight);
        EXPECT_EQ(finalCompressedSize, totalCompressedSize);
        EXPECT_EQ(finalUncompressedSize, totalUncompressedSize);
    }
}

class TReaderInterruptionsParametrized
    : public TReaderInterruptions
    , public testing::WithParamInterface<std::tuple<bool, bool, int>>
{ };

TEST_P(TReaderInterruptionsParametrized, MultiChunkReaderAfterEachRow)
{
    bool isParallel = std::get<0>(GetParam());
    bool withOverrides = std::get<1>(GetParam());
    int rowCountOverride = std::get<2>(GetParam());

    auto chunkSpec = WriteBatchOfRows({1, 2, 3, 4, 5});
    auto miscExt = FetchMiscExt(FromProto<TChunkId>(chunkSpec.chunk_id()));

    chunkSpec.set_row_count_override(rowCountOverride);

    i64 expectedDataWeight = miscExt.data_weight();
    i64 expectedCompressedSize = miscExt.compressed_data_size();
    i64 expectedUncompressedSize = miscExt.uncompressed_data_size();

    if (withOverrides) {
        chunkSpec.set_data_weight_override(miscExt.data_weight() * 2);
        chunkSpec.set_compressed_data_size_override(miscExt.compressed_data_size() * 3);
        chunkSpec.set_uncompressed_data_size_override(miscExt.uncompressed_data_size() * 4);

        expectedDataWeight = chunkSpec.data_weight_override();
        expectedCompressedSize = chunkSpec.compressed_data_size_override();
        expectedUncompressedSize = chunkSpec.uncompressed_data_size_override();
    }

    auto sourceDirectory = New<TDataSourceDirectory>();
    auto& source = sourceDirectory->DataSources().emplace_back(New<TDataSource>());
    source->Schema() = Schema_;

    auto chunkReaderHost = TChunkReaderHost::FromClient(NativeClient_);
    auto multiChunkReaderHost = CreateSingleSourceMultiChunkReaderHost(chunkReaderHost);

    ISchemalessMultiChunkReaderPtr reader = isParallel
        ? CreateSchemalessParallelMultiReader(
            New<TTableReaderConfig>(),
            New<TTableReaderOptions>(),
            multiChunkReaderHost,
            sourceDirectory,
            {TDataSliceDescriptor(chunkSpec)},
            /*hintKeyPrefixes*/ std::nullopt,
            NameTable_,
            /*chunkReadOptions*/ {},
            TReaderInterruptionOptions::InterruptibleWithEmptyKey())
        : CreateSchemalessSequentialMultiReader(
            New<TTableReaderConfig>(),
            New<TTableReaderOptions>(),
            multiChunkReaderHost,
            sourceDirectory,
            {TDataSliceDescriptor(chunkSpec)},
            /*hintKeyPrefixes*/ std::nullopt,
            NameTable_,
            /*chunkReadOptions*/ {},
            TReaderInterruptionOptions::InterruptibleWithEmptyKey());

    WaitFor(reader->GetReadyEvent())
        .ThrowOnError();

    int totalRows = rowCountOverride;
    int upperRowIndex = miscExt.row_count();

    VerifyInterruptDescriptor(
        reader->GetInterruptDescriptor({}),
        /*expectedRead*/ std::nullopt,
        /*expectedUnread*/ TExpectedSliceDescriptor{
            .RowCount = totalRows,
            .DataWeight = expectedDataWeight,
            .CompressedDataSize = expectedCompressedSize,
            .UncompressedDataSize = expectedUncompressedSize,
        });

    int rowsRead = 0;

    auto computeExpectedDescriptors = [&](int effectiveRowsRead, bool hasUnreadInBatch) {
        std::optional<TExpectedSliceDescriptor> expectedRead;
        std::optional<TExpectedSliceDescriptor> expectedUnread;

        if (effectiveRowsRead > 0) {
            expectedRead = TExpectedSliceDescriptor{.RowCount = effectiveRowsRead};
        }

        if (effectiveRowsRead < totalRows) {
            double readFactor = static_cast<double>(effectiveRowsRead) / totalRows;
            double unreadFactor = static_cast<double>(totalRows - effectiveRowsRead) / totalRows;

            if (expectedRead) {
                expectedRead->DataWeight = static_cast<i64>(expectedDataWeight * readFactor);
                expectedRead->CompressedDataSize = static_cast<i64>(expectedCompressedSize * readFactor);
                expectedRead->UncompressedDataSize = static_cast<i64>(expectedUncompressedSize * readFactor);
            }

            expectedUnread = TExpectedSliceDescriptor{
                .RowCount = totalRows - effectiveRowsRead,
                .DataWeight = static_cast<i64>(expectedDataWeight * unreadFactor),
                .CompressedDataSize = static_cast<i64>(expectedCompressedSize * unreadFactor),
                .UncompressedDataSize = static_cast<i64>(expectedUncompressedSize * unreadFactor),
                .HasLowerKey = hasUnreadInBatch,
            };
        } else if (effectiveRowsRead < upperRowIndex) {
            int unreadRowCount = std::max(1, std::min(
                totalRows - effectiveRowsRead + (hasUnreadInBatch ? static_cast<int>(rowsRead - effectiveRowsRead) : 0),
                upperRowIndex - effectiveRowsRead));

            expectedUnread = TExpectedSliceDescriptor{
                .RowCount = unreadRowCount,
                .HasLowerKey = hasUnreadInBatch,
            };
        }

        return std::make_pair(expectedRead, expectedUnread);
    };

    while (auto batch = reader->Read()) {
        if (batch->IsEmpty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        auto rows = batch->MaterializeRows();

        for (size_t i = 0; i < rows.size(); ++i) {
            auto unreadRowsInBatch = TRange<TUnversionedRow>(rows.data() + i, rows.data() + rows.size());
            int effectiveRowsRead = rowsRead + rows.size() - unreadRowsInBatch.Size();

            auto [expectedRead, expectedUnread] = computeExpectedDescriptors(effectiveRowsRead, /*hasUnreadInBatch*/ true);
            VerifyInterruptDescriptor(reader->GetInterruptDescriptor(unreadRowsInBatch), expectedRead, expectedUnread);
        }

        rowsRead += rows.size();

        auto [expectedRead, expectedUnread] = computeExpectedDescriptors(rowsRead, /*hasUnreadInBatch*/ false);
        VerifyInterruptDescriptor(reader->GetInterruptDescriptor({}), expectedRead, expectedUnread);
    }

    EXPECT_EQ(rowsRead, miscExt.row_count());
}

INSTANTIATE_TEST_SUITE_P(
    ReaderTypes,
    TReaderInterruptionsParametrized,
    Combine(
        /*isParallel*/ Bool(),
        /*withOverrides*/ Bool(),
        /*rowCountOverride*/ Values(1, 2, 3, 5)
    ));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
