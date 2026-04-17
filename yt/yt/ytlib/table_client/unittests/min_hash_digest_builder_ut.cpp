#include <yt/yt/ytlib/table_client/min_hash_digest_builder.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/min_hash_digest/config.h>
#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 MakeTimestamp(ui64 index)
{
    return index << 30;
}

TEST(TMinHashDigestBuilder, Simple)
{
    auto digestBuilder = CreateMinHashDigestBuilder(New<TMinHashDigestConfig>());

    auto addRow = [&digestBuilder] (ui64 index, bool addDeleteTimestamp = false) {
        TVersionedRowBuilder builder(New<TRowBuffer>());

        builder.AddKey(MakeUnversionedInt64Value(index));
        builder.AddValue(MakeVersionedStringValue("b", MakeTimestamp(index), 1));
        builder.AddValue(MakeVersionedInt64Value(1, MakeTimestamp(index * 2), 2));
        if (addDeleteTimestamp) {
            builder.AddDeleteTimestamp(MakeTimestamp(index * 3));
        }

        digestBuilder->OnRow(builder.FinishRow());
    };

    for (int index = 1; index <= 100; ++index) {
        addRow(index, /*addDeleteTimestamp*/ true);
    }

    for (int index = 1; index <= 10; ++index) {
        addRow(index);
    }

    NProto::TSystemBlockMetaExt systemBlockMeta;
    auto data = digestBuilder->SerializeBlock(&systemBlockMeta);

    auto meta = systemBlockMeta
        .system_blocks(0)
        .GetExtension(NProto::TMinHashDigestSystemBlockMeta::min_hash_digest_block_meta);

    auto digest = New<TMinHashDigest>(GetNullMemoryUsageTracker());
    digest->Initialize(data);

    auto retentionConfig = New<TRetentionConfig>();
    auto similarityConfig = New<TMinHashSimilarityConfig>();
    similarityConfig->MinSimilarity = 0.501;

    auto timestamp = digest->CalculateWriteDeleteSimilarityTimestamp(similarityConfig);
    // Magic number, which is correct timestamp for certain input.
    auto expectedTimestamp = TInstant::Seconds(153);

    ASSERT_EQ(TInstant::Seconds(timestamp), expectedTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
