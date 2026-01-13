#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/server/lib/s3/public.h>
#include <yt/yt/server/lib/s3/config.h>
#include <yt/yt/server/lib/s3/chunk_writer.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/library/s3/public.h>
#include <yt/yt/library/s3/credential_provider.h>
#include <yt/yt/library/s3/config.h>
#include <yt/yt/library/s3/client.h>

#include <util/system/env.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSignature;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NYson;

using ::testing::HasSubstr;
using ::testing::SizeIs;

////////////////////////////////////////////////////////////////////////////////

// const auto Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomString(size_t size, TRandomGenerator* generator)
{
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = generator->Generate<ui64>();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

std::vector<TBlock> CreateBlocks(int count, TRandomGenerator* generator)
{
    std::vector<TBlock> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; index++) {
        int size = 10 + generator->Generate<uint>() % 11;
        blocks.push_back(TBlock(TSharedRef::FromString(GenerateRandomString(size, generator))));
    }

    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

class TS3DataTest
    : public TApiTestBase
{
protected:
    const TString RootBucket_ = "ytsaurus";
    TRandomGenerator Generator_ = TRandomGenerator(42);
    std::vector<TBlock> GeneratedBlocks_;

    NS3::IClientPtr S3Client_;

    IChunkReaderPtr S3ChunkReader_;
    IChunkWriterPtr S3ChunkWriter_;
    TChunkId ChunkId_;
    TS3MediumDescriptorPtr MediumDescriptor_;

    NNative::IClientPtr NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
    IChunkReaderAllowingRepairPtr ReplicationReader_;

    void SetUpS3Client()
    {
        // The following environment variables are expected for the test to work.
        auto endpointUrl = GetEnv("AWS_ENDPOINT_URL");
        auto region = GetEnv("AWS_REGION");
        auto accessKeyId = GetEnv("AWS_ACCESS_KEY_ID");
        auto secretAccessKey = GetEnv("AWS_SECRET_ACCESS_KEY");
        if (endpointUrl.empty() || region.empty() || accessKeyId.empty() || secretAccessKey.empty()) {
            GTEST_FAIL() << "S3 environment is not configured; check if the local_s3_recipe is included";
        }

        auto clientConfig = New<NS3::TS3ClientConfig>();
        clientConfig->Url = endpointUrl;
        clientConfig->Region = region;

        auto s3CredentialProvider = NS3::CreateStaticCredentialProvider(accessKeyId, secretAccessKey);
        auto poller = CreateThreadPoolPoller(1, "S3TestPoller");
        S3Client_ = NS3::CreateClient(
            std::move(clientConfig),
            std::move(s3CredentialProvider),
            /*sslContextConfig*/ nullptr,
            poller,
            poller->GetInvoker());

        WaitFor(S3Client_->Start())
            .ThrowOnError();
    }

    void SetUpS3Writer()
    {
        YT_VERIFY(S3Client_);
        YT_VERIFY(MediumDescriptor_);

        auto writerConfig = New<NS3::TS3WriterConfig>();
        S3ChunkWriter_ = CreateS3RegularChunkWriter(
            S3Client_,
            MediumDescriptor_,
            std::move(writerConfig),
            TSessionId{ChunkId_, MediumDescriptor_->GetIndex()});
    }

    void SetUpReplicationReader()
    {
        ReplicationReader_ = CreateReplicationReader(
            New<TReplicationReaderConfig>(),
            New<TRemoteReaderOptions>(),
            TChunkReaderHost::FromClient(NativeClient_),
            ChunkId_,
            TChunkReplicaWithMediumList{TChunkReplicaWithMedium(
                OffshoreNodeId,
                1,
                MediumDescriptor_->GetIndex()
            )}
        );
    }

    void SetUp() override
    {
        GeneratedBlocks_ = CreateBlocks(1024, &Generator_);
        EXPECT_EQ(std::ssize(GeneratedBlocks_), 1024);

        ChunkId_ = MakeRandomId(EObjectType::Chunk, TCellTag(0xf003));
        auto mediumConfig = New<TS3MediumConfig>();
        mediumConfig->Bucket = RootBucket_;
        MediumDescriptor_ = New<TS3MediumDescriptor>(
            /*name*/ "test_s3_medium",
            /*index*/ 15,
            /*priority*/ 0,
            std::move(mediumConfig));

        SetUpS3Client();
        CleanBuckets();
        SetUpS3Writer();
        SetUpReplicationReader();

        WaitFor(S3Client_->PutBucket({
            .Bucket = RootBucket_,
        })).ThrowOnError();
    }

    void TearDown() override
    {
        CleanBuckets();
    }

    void CleanBuckets()
    {
        if (!S3Client_) {
            // It means that we have skipped this test suite.
            return;
        }

        // Clean all the objects and buckets up.
        auto listBucketsRsp = WaitFor(S3Client_->ListBuckets({}))
            .ValueOrThrow();
        for (const auto& bucket: listBucketsRsp.Buckets) {
            auto listObjectsResponse = WaitFor(S3Client_->ListObjects({
                .Bucket = bucket.Name,
            }))
                .ValueOrThrow();

            std::vector<TString> objectKeys;
            for (const auto& object: listObjectsResponse.Objects) {
                objectKeys.push_back(object.Key);
            }
            if (!objectKeys.empty()) {
                WaitFor(S3Client_->DeleteObjects({
                    .Bucket = bucket.Name,
                    .Objects = std::move(objectKeys),
                }))
                    .ValueOrThrow();
            }

            WaitFor(S3Client_->DeleteBucket({
                bucket.Name,
            }))
                .ValueOrThrow();
        }
    }
};

TEST_F(TS3DataTest, TestReplicationReader)
{
    IChunkWriter::TWriteBlocksOptions writeOptions;
    TWorkloadDescriptor workloadDescriptor;
    S3ChunkWriter_->Open()
        .Apply(BIND([&] {
            EXPECT_TRUE(S3ChunkWriter_->WriteBlocks(writeOptions, workloadDescriptor, GeneratedBlocks_));
            return S3ChunkWriter_->GetReadyEvent();
        }))
        .Apply(BIND([&] {
            auto deferredMeta = New<TDeferredChunkMeta>();
            deferredMeta->set_type(0);
            deferredMeta->set_format(0);
            *deferredMeta->mutable_extensions() = {};
            return S3ChunkWriter_->Close({}, {}, deferredMeta);
        }))
        .Wait(TDuration::Seconds(120));

    IChunkReader::TReadBlocksOptions readOptions;
    auto readBlocks = WaitFor(ReplicationReader_->ReadBlocks(readOptions, {1}))
        .ValueOrThrow();
}

// TEST_F(TAlterTableTest, TestUnknownType)
// {
//     auto createRes = Client_->CreateNode("//tmp/t1", EObjectType::Table);
//     WaitFor(createRes)
//         .ThrowOnError();

//     {
//         // Type not set.
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "required fileds are not set");
//     }

//     {
//         // Type is bad.
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(ToProto(EValueType::Min));

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "has no corresponding logical type");
//     }

//     {
//         // Type is unknown.
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(-1);

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "Error casting");
//     }

//     {
//         // Simple type is unknown.
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(ToProto(EValueType::Any));
//         column->set_simple_logical_type(-1);

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "Error casting");
//     }

//     {
//         // Mismatch of type and simple logical type.
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(ToProto(EValueType::Any));
//         column->set_simple_logical_type(ToProto(ESimpleLogicalValueType::Int64));

//         EXPECT_NO_THROW(AlterTable("//tmp/t1", schema));
//     }

//     {
//         // Unknown simple type in type_v3
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(ToProto(EValueType::Int64));
//         column->mutable_logical_type()->set_simple(-1);

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "Error casting");
//     }

//     {
//         // Unset type in type_v3
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(ToProto(EValueType::Int64));
//         column->mutable_logical_type();

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "Cannot parse unknown logical type from proto");
//     }

//     {
//         // Unknown type in type_v3
//         NTableClient::NProto::TTableSchemaExt schema;
//         auto* column = schema.add_columns();
//         column->set_name("foo");
//         column->set_stable_name("foo");
//         column->set_type(ToProto(EValueType::Int64));
//         column->mutable_logical_type();
//         auto unknownFields = column->GetReflection()->MutableUnknownFields(column);
//         unknownFields->AddVarint(100500, 0);

//         EXPECT_THROW_WITH_SUBSTRING(
//             AlterTable("//tmp/t1", schema),
//             "Cannot parse unknown logical type from proto");
//     }
// }

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
