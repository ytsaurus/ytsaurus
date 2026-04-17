#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/patchable_field.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>
#include <util/system/tempfile.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

// A simple mapper that passes rows through unchanged.
class TDoNothingMapper : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (; reader->IsValid(); reader->Next()) {
            writer->AddRow(reader->GetRow());
        }
    }
};
REGISTER_MAPPER(TDoNothingMapper)

////////////////////////////////////////////////////////////////////////////////

// This test verifies that setting file_cache_replication_factor in the remote
// cluster config causes files uploaded for an operation to be stored with the
// configured replication factor.
TEST(TFileCacheReplicationFactorTest, AffectsFileUploadViaOperation)
{
    auto config = MakeIntrusive<TConfig>();
    TTestFixture fixture(TCreateClientOptions().Config(config));

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    // Set up the remote config document that the client reads dynamic config from.
    TYPath configPath = workingDir + "/client_config";
    TYPath configProfilePath = configPath + "/" + TPatchableField<ui64>::ConfigProfile;

    client->Create(configPath, ENodeType::NT_MAP);
    client->Create(configProfilePath, ENodeType::NT_DOCUMENT);

    // Drop local mode: in local mode the cluster always forces replication_factor=1,
    // which would mask the effect we are testing.
    auto localModeFqdn = client->Get("//sys/@local_mode_fqdn").AsString();
    client->Remove("//sys/@local_mode_fqdn");
    // Restore local_mode_fqdn on scope exit so subsequent tests are not affected
    // even if this test fails or throws.
    Y_DEFER {
        client->Set("//sys/@local_mode_fqdn", localModeFqdn);
    };

    config->ConfigRemotePatchPath = configPath;

    // Set file_cache_replication_factor to 2 via the remote cluster config.
    client->Set(configProfilePath, TNode()("file_cache_replication_factor", 2));

    // Create a minimal input table so the Map operation has something to process.
    TYPath inputTable = workingDir + "/input";
    TYPath outputTable = workingDir + "/output";
    {
        auto writer = client->CreateTableWriter<TNode>(inputTable);
        writer->AddRow(TNode()("key", "value"));
        writer->Finish();
    }

    // Create a temporary local file to attach to the operation.
    // Using unique content ensures a cache miss so the file is actually uploaded.
    TTempFile tempFile(MakeTempName());
    {
        TOFStream os(tempFile.Name());
        os << "file content for replication factor test: " << CreateGuidAsString() << "\n";
    }

    // Run a Map operation with the local file attached via MapperSpec.
    // The library will upload the file to the file cache with the configured
    // replication factor before starting the operation.
    auto operation = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>(inputTable)
            .AddOutput<TNode>(outputTable)
            .MapperSpec(TUserJobSpec()
                .AddLocalFile(tempFile.Name(), TAddLocalFileOptions().PathInJob("test_file.txt"))),
        new TDoNothingMapper());

    // Retrieve the full operation spec from the cluster.
    auto fullSpec = *operation->GetAttributes().FullSpec;

    // The mapper spec contains "file_paths" — the list of Cypress paths for all
    // files that were uploaded for this job (including the job binary itself).
    auto& mapperSpec = fullSpec["mapper"];
    ASSERT_TRUE(mapperSpec.HasKey("file_paths"))
        << "mapper spec does not contain file_paths";

    TVector<TRichYPath> filePaths;
    Deserialize(filePaths, mapperSpec["file_paths"]);
    ASSERT_FALSE(filePaths.empty())
        << "file_paths list is empty";

    // Every file uploaded for this operation should have replication_factor == 2.
    for (const auto& richPath : filePaths) {
        auto replicationFactor = client->Get(richPath.Path_ + "/@replication_factor").AsInt64();
        EXPECT_EQ(replicationFactor, 2)
            << "File " << richPath.Path_ << " has unexpected replication_factor";
    }

}
