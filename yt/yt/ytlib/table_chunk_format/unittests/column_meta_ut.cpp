#include <yt/yt/ytlib/table_chunk_format/column_meta.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NTableChunkFormat {

namespace {

using namespace NTableClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TEST(TColumnMetaTest, SegmentMetaExtensionsCount)
{
    NProto::TSegmentMeta segmentMeta;
    auto* descriptor = segmentMeta.GetDescriptor();
    const google::protobuf::DescriptorPool* descriptorPool = descriptor->file()->pool();;
    std::vector<const google::protobuf::FieldDescriptor*> allExtensions;
    descriptorPool->FindAllExtensions(descriptor, &allExtensions);

    // This will remind keeping TSegmentMeta syncronyzed with NProto::TSegmentMeta.
    EXPECT_EQ(TSegmentMeta::GetExtensionCount(), ssize(allExtensions));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TColumnMetaTest, SegmentMetaCorrectParse)
{
    auto expectedSegmentMeta = CreateSimpleSegmentMeta();
    auto parsedSegmentMeta = FromProto<TSegmentMeta>(expectedSegmentMeta);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(ToProto<NProto::TSegmentMeta>(parsedSegmentMeta), expectedSegmentMeta));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TColumnMetaTest, ColumnMetaCorrectParse)
{
    NProto::TColumnMeta expectedColumnMeta;
    for (int i = 0; i < 5; ++i) {
        *expectedColumnMeta.add_segments() = CreateSimpleSegmentMeta();
    }
    auto parsedColumnMeta = FromProto<TColumnMeta>(expectedColumnMeta);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(ToProto<NProto::TColumnMeta>(parsedColumnMeta), expectedColumnMeta));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableChunkFormat
