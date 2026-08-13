#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/table_client/dictionary_compression_session.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/performance_counters.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/checksum.h>

#include <cstring>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TMockChunkFragmentReader
    : public IChunkFragmentReader
{
public:
    explicit TMockChunkFragmentReader(std::vector<TSharedRef> fragments)
        : Fragments_(std::move(fragments))
    { }

    TFuture<TReadFragmentsResponse> ReadFragments(
        std::vector<TChunkFragmentRequest> requests,
        TClientChunkReadOptions /*options*/) override
    {
        EXPECT_EQ(requests.size(), Fragments_.size());

        TReadFragmentsResponse response;
        response.Fragments = Fragments_;
        return MakeFuture(std::move(response));
    }

private:
    const std::vector<TSharedRef> Fragments_;
};

class TMockDictionaryDecompressionSession
    : public IDictionaryDecompressionSession
{
public:
    explicit TMockDictionaryDecompressionSession(std::vector<std::string> decompressedValues)
        : DecompressedValues_(std::move(decompressedValues))
    { }

    TFuture<std::vector<TSharedRef>> DecompressValues(
        TSharedRange<TUnversionedValue*> values,
        std::vector<TChunkId> dictionaryIds,
        TClientChunkReadOptions /*chunkReadOptions*/) override
    {
        EXPECT_EQ(values.size(), DecompressedValues_.size());
        EXPECT_EQ(dictionaryIds.size(), DecompressedValues_.size());

        std::vector<TSharedRef> results;
        results.reserve(DecompressedValues_.size());
        for (int index = 0; index < std::ssize(DecompressedValues_); ++index) {
            auto result = TSharedRef::FromString(DecompressedValues_[index]);
            values[index]->Data.String = result.Begin();
            values[index]->Length = result.Size();
            results.push_back(std::move(result));
        }

        return MakeFuture(std::move(results));
    }

    TDuration GetDecompressionTime() const override
    {
        return {};
    }

private:
    const std::vector<std::string> DecompressedValues_;
};

class TMockDictionaryCompressionFactory
    : public IDictionaryCompressionFactory
{
public:
    explicit TMockDictionaryCompressionFactory(std::vector<std::string> decompressedValues)
        : DecompressedValues_(std::move(decompressedValues))
    { }

    TFuture<IDictionaryCompressionSessionPtr> MaybeCreateDictionaryCompressionSession(
        const TClientChunkReadOptions& /*chunkReadOptions*/,
        std::optional<TChunkId> /*presetCompressionDictionaryId*/) const override
    {
        return MakeFuture(IDictionaryCompressionSessionPtr{});
    }

    IDictionaryDecompressionSessionPtr CreateDictionaryDecompressionSession() override
    {
        return New<TMockDictionaryDecompressionSession>(DecompressedValues_);
    }

    TFuture<THashMap<TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TClientChunkReadOptions& /*chunkReadOptions*/,
        const THashSet<TChunkId>& /*dictionaryIds*/) override
    {
        return MakeFuture(THashMap<TChunkId, TRowDictionaryDecompressor>{});
    }

private:
    const std::vector<std::string> DecompressedValues_;
};

TSharedRef MakeHunkFragment(TStringBuf payload)
{
    auto fragment = TSharedMutableRef::Allocate(sizeof(THunkPayloadHeader) + payload.size());
    const THunkPayloadHeader header{
        .Checksum = GetChecksum(TRef(payload.data(), payload.size())),
    };
    std::memcpy(fragment.Begin(), &header, sizeof(header));
    std::memcpy(fragment.Begin() + sizeof(header), payload.data(), payload.size());
    return fragment;
}

////////////////////////////////////////////////////////////////////////////////

TEST(THunkDecodingTest, MixedCompressedAndUncompressedDataWeight)
{
    const std::string uncompressedPayload = "plain hunk";
    const std::string compressedPayload = "compressed referenced hunk";
    const std::string compressedInlinePayload = "compressed inline hunk";
    const std::string decompressedReferencedPayload = "decompressed referenced hunk value";
    const std::string decompressedInlinePayload = "decompressed inline hunk value";

    const auto cellTag = TCellTag(0x42);
    const auto uncompressedChunkId = MakeRandomId(EObjectType::Chunk, cellTag);
    const auto compressedChunkId = MakeRandomId(EObjectType::Chunk, cellTag);
    const auto dictionaryChunkId = MakeRandomId(EObjectType::Chunk, cellTag);

    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(3);

    auto uncompressedRef = WriteHunkValue(
        rowBuffer->GetPool(),
        TGlobalRefHunkValue{
            .ChunkId = uncompressedChunkId,
            .ErasureCodec = NErasure::ECodec::None,
            .BlockIndex = 0,
            .BlockOffset = 0,
            .Length = std::ssize(uncompressedPayload),
        });
    row[0] = MakeUnversionedStringValue(uncompressedRef.ToStringBuf(), 0, EValueFlags::Hunk);

    auto compressedRef = WriteHunkValue(
        rowBuffer->GetPool(),
        TGlobalRefHunkValue{
            .ChunkId = compressedChunkId,
            .ErasureCodec = NErasure::ECodec::None,
            .BlockIndex = 0,
            .BlockOffset = 0,
            .Length = std::ssize(compressedPayload),
            .CompressionDictionaryId = dictionaryChunkId,
        });
    row[1] = MakeUnversionedStringValue(compressedRef.ToStringBuf(), 1, EValueFlags::Hunk);

    auto compressedInlineRef = WriteHunkValue(
        rowBuffer->GetPool(),
        TCompressedInlineRefHunkValue{
            .CompressionDictionaryId = dictionaryChunkId,
            .Payload = TRef(compressedInlinePayload.data(), compressedInlinePayload.size()),
        });
    row[2] = MakeUnversionedStringValue(compressedInlineRef.ToStringBuf(), 2, EValueFlags::Hunk);

    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("uncompressed", EValueType::String).SetMaxInlineHunkSize(1),
        TColumnSchema("compressed", EValueType::String).SetMaxInlineHunkSize(1),
        TColumnSchema("compressed_inline", EValueType::String).SetMaxInlineHunkSize(1),
    });

    TClientChunkReadOptions options;
    options.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive);
    options.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
        /*enableHunkColumnarProfiling*/ false,
        schema);
    auto hunkChunkReaderStatistics = options.HunkChunkReaderStatistics;

    auto performanceCounters = New<TTabletPerformanceCounters>();
    auto decodedRows = WaitFor(DecodeHunksInSchemafulUnversionedRows(
        schema,
        TColumnFilter::MakeUniversal(),
        New<TMockChunkFragmentReader>(std::vector{
            MakeHunkFragment(uncompressedPayload),
            MakeHunkFragment(compressedPayload),
        }),
        New<TMockDictionaryCompressionFactory>(std::vector{
            decompressedReferencedPayload,
            decompressedInlinePayload,
        }),
        std::move(options),
        performanceCounters,
        MakeSharedRange(std::vector{row}, rowBuffer)))
        .ValueOrThrow();

    ASSERT_EQ(decodedRows.size(), 1u);
    auto decodedRow = decodedRows[0];
    EXPECT_EQ(TStringBuf(decodedRow[0].Data.String, decodedRow[0].Length), uncompressedPayload);
    EXPECT_EQ(TStringBuf(decodedRow[1].Data.String, decodedRow[1].Length), decompressedReferencedPayload);
    EXPECT_EQ(TStringBuf(decodedRow[2].Data.String, decodedRow[2].Length), decompressedInlinePayload);

    const i64 expectedDataWeight =
        sizeof(THunkPayloadHeader) + uncompressedPayload.size() +
        decompressedReferencedPayload.size() +
        decompressedInlinePayload.size();
    EXPECT_EQ(hunkChunkReaderStatistics->DataWeight().load(), expectedDataWeight);
    EXPECT_EQ(
        performanceCounters->StaticHunkChunkRowLookupDataWeight.Counter.load(),
        expectedDataWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
