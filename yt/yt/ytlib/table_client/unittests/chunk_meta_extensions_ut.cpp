#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NTableClient {

namespace {

TEST(TChunkMetaExtensionsTest, BoundaryKeysExtParsing)
{
    NProto::TBoundaryKeysExt expectedBoundaryKeysExt;
    expectedBoundaryKeysExt.set_min("MIN");
    expectedBoundaryKeysExt.set_max("MAX");
    auto parsedBoundaryKeys = NYT::FromProto<TBoundaryKeysExtension>(expectedBoundaryKeysExt);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(NYT::ToProto<NProto::TBoundaryKeysExt>(parsedBoundaryKeys), expectedBoundaryKeysExt));
}

TEST(TChunkMetaExtensionsTest, ColumnMetaExtensionParsing)
{
    NProto::TColumnMetaExt expectedColumnMetaExt;
    auto* column = expectedColumnMetaExt.add_columns();
    *column->add_segments() = CreateSimpleSegmentMeta();

    auto parsedColumnMeta = NYT::FromProto<TColumnMetaExtension>(expectedColumnMetaExt);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(NYT::ToProto<NProto::TColumnMetaExt>(parsedColumnMeta), expectedColumnMetaExt));
}

TEST(TChunkMetaExtensionsTest, KeyColumnsExtensionParsing)
{
    NProto::TKeyColumnsExt expectedKeyColumnsExt;
    *expectedKeyColumnsExt.add_names() = "k1";
    *expectedKeyColumnsExt.add_names() = "k2";
    *expectedKeyColumnsExt.add_names() = "k3";
    auto parsedKeyColumns = NYT::FromProto<TKeyColumnsExtension>(expectedKeyColumnsExt);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(NYT::ToProto<NProto::TKeyColumnsExt>(parsedKeyColumns), expectedKeyColumnsExt));
}

TEST(TChunkMetaExtensionsTest, SamplesExtensionParsing)
{
    NProto::TSamplesExt expectedSamplesExt;
    *expectedSamplesExt.add_entries() = "e1";
    *expectedSamplesExt.add_entries() = "e2";
    *expectedSamplesExt.add_entries() = "e3";

    expectedSamplesExt.add_weights(1);
    expectedSamplesExt.add_weights(2);
    expectedSamplesExt.add_weights(3);
    auto parsedSamples = NYT::FromProto<TSamplesExtension>(expectedSamplesExt);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(NYT::ToProto<NProto::TSamplesExt>(parsedSamples), expectedSamplesExt));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
