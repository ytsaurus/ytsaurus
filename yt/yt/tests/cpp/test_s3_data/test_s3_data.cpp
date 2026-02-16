#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/server/lib/s3/public.h>
#include <yt/yt/server/lib/s3/config.h>
#include <yt/yt/server/lib/s3/chunk_writer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>
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

    NNative::IClientPtr NativeClient_;
    IChunkReaderAllowingRepairPtr ReplicationReader_;

    void SetUpS3ClientAndMedium()
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

        auto mediumConfig = New<TS3MediumConfig>();
        mediumConfig->Bucket = RootBucket_;
        mediumConfig->Region = region;
        mediumConfig->Url = endpointUrl;
        MediumDescriptor_ = New<TS3MediumDescriptor>(
            /*name*/ "test_s3_medium",
            /*index*/ 15,
            /*priority*/ 0,
            mediumConfig);

        TCreateObjectOptions options;
        auto attrs = CreateEphemeralAttributes();
        attrs->Set("name", "test_s3_medium");
        attrs->Set("index", 15);
        attrs->Set("priority", 0);
        attrs->Set("config", mediumConfig);
        options.Attributes = attrs;

        WaitFor(Client_->CreateObject(EObjectType::S3Medium, options))
            .ThrowOnError();
        WaitUntil(
            [&] {
                return WaitFor(Client_->NodeExists("//sys/media/test_s3_medium"))
                    .ValueOrThrow();
            },
            "The S3 medium was not created");
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
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
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

        SetUpS3ClientAndMedium();
        CleanBuckets();
        SetUpS3Writer();
        SetUpReplicationReader();

        WaitFor(S3Client_->PutBucket({
            .Bucket = RootBucket_,
        })).ThrowOnError();

        WaitUntil(
            [&] {
                auto value = WaitFor(Client_->GetNode("//sys/offshore_data_gateways/instances/@count"))
                    .ValueOrDefault(ConvertToYsonString(0));
                return ConvertTo<i64>(value) >= 1;
            },
            "Offshore data gateway(s) have not launched");
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
    WaitFor(S3ChunkWriter_->Open()
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
        })))
        .ThrowOnError();

    // TODO(pavel-bash): when the master supports offshore media, it'll be possible to continue this test; now
    // it's stopped at "Mediu 15 is not an S3 medium" because master does not serialize an offshore medium as
    // an offshore one; see a TODO in SerializeMediumDirectory.
    IChunkReader::TReadBlocksOptions readOptions;
    auto readBlocks = WaitFor(ReplicationReader_->ReadBlocks(readOptions, {1}))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
