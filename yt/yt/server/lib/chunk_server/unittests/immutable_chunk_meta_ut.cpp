#include <yt/yt/server/lib/chunk_server/immutable_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/system/sanitizers.h>

namespace NYT::NChunkServer {
namespace {

using namespace NChunkClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TEST(TImmutableChunkMetaTest, Null)
{
    auto meta = TImmutableChunkMeta::CreateNull();
    EXPECT_EQ(EChunkType::Unknown, meta->GetType());
    EXPECT_EQ(EChunkFormat::Unknown, meta->GetFormat());
    EXPECT_EQ(EChunkFeatures::None, meta->GetFeatures());
    EXPECT_EQ(0, meta->GetExtensionCount());
    EXPECT_EQ(0, meta->GetExtensionsByteSize());
}

NChunkClient::NProto::TChunkMeta MakeProtoMetaWithExtensions()
{
    NChunkClient::NProto::TChunkMeta protoMeta;
    protoMeta.set_type(ToProto<int>(EChunkType::Table));
    protoMeta.set_format(ToProto<int>(EChunkFormat::TableVersionedColumnar));
    protoMeta.set_features(ToProto<ui64>(EChunkFeatures::DescendingSortOrder));

    NChunkClient::NProto::TMiscExt miscExt;
    miscExt.set_row_count(100);
    miscExt.set_data_weight(200);
    SetProtoExtension(protoMeta.mutable_extensions(), miscExt);

    NTableClient::NProto::TBoundaryKeysExt boundaryKeysExt;
    boundaryKeysExt.set_min("MIN");
    boundaryKeysExt.set_max("MAX");
    SetProtoExtension(protoMeta.mutable_extensions(), boundaryKeysExt);

    return protoMeta;
}

TEST(TImmutableChunkMetaTest, ProtoConversion)
{
    auto protoMeta1 = MakeProtoMetaWithExtensions();

    auto meta = FromProto<TImmutableChunkMetaPtr>(protoMeta1);

    EXPECT_EQ(EChunkType::Table, meta->GetType());
    EXPECT_EQ(EChunkFormat::TableVersionedColumnar, meta->GetFormat());
    EXPECT_EQ(EChunkFeatures::DescendingSortOrder, meta->GetFeatures());
    EXPECT_EQ(2, meta->GetExtensionCount());

    int extensionsByteSize = 0;
    for (const auto& extension : protoMeta1.extensions().extensions()) {
        extensionsByteSize += static_cast<int>(extension.data().size());
    }
    EXPECT_EQ(extensionsByteSize, meta->GetExtensionsByteSize());

    auto protoMeta2 = ToProto<NChunkClient::NProto::TChunkMeta>(meta);
    EXPECT_EQ(protoMeta1.DebugString(), protoMeta2.DebugString());
}

TEST(TImmutableChunkMetaTest, ExtensionFiltering)
{
    auto protoMeta1 = MakeProtoMetaWithExtensions();

    auto meta = FromProto<TImmutableChunkMetaPtr>(protoMeta1);

    NChunkClient::NProto::TChunkMeta protoMeta2;
    THashSet<int> tags{
        TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value
    };
    ToProto(&protoMeta2, meta, &tags);

    EXPECT_EQ(1, protoMeta2.extensions().extensions_size());
    EXPECT_EQ(
        GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(protoMeta1.extensions()).DebugString(),
        GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(protoMeta2.extensions()).DebugString());
}

#if !defined(_asan_enabled_) && !defined(_msan_enabled_)

TEST(TImmutableChunkMetaTest, TotalByteSize)
{
    auto protoMeta = MakeProtoMetaWithExtensions();

    auto meta = FromProto<TImmutableChunkMetaPtr>(protoMeta);

    auto payloadSize = sizeof(TImmutableChunkMeta) + meta->GetExtensionsByteSize();

    EXPECT_GE(static_cast<size_t>(meta->GetTotalByteSize()), payloadSize);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
