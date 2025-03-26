#include <yt/cpp/mapreduce/library/parallel_io/parallel_file_writer.h>
#include <yt/cpp/mapreduce/library/parallel_io/resource_limiter.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/string/vector.h>

#include <util/system/tempfile.h>

using namespace NYT;
using namespace NYT::NTesting;

void TryWriteFile(TParallelFileWriterOptions& options)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto fileName = "temp_ParallelFileWriter";
    TFile file(fileName, WrOnly | CreateAlways);
    TFileOutput output(file);
    output << GenerateRandomData(10 * 1024 * 1024);
    output.Flush();
    WriteFileParallel(client, fileName, workingDir + "/file", options);
}

TEST(TParallelFileWriterTest, InvalidWriterOptionsFail)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TryWriteFile(TParallelFileWriterOptions().WriterOptions(
            TWriterOptions()
                .UploadReplicationFactor(0))),
        NYT::TErrorResponse,
        "/upload_replication_factor");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TryWriteFile(TParallelFileWriterOptions().WriterOptions(
            TWriterOptions()
                .MinUploadReplicationFactor(0))),
        NYT::TErrorResponse,
        "/min_upload_replication_factor");
}

TEST(TParallelFileWriterTest, JustWriteFile)
{
    TTestFixture fixture;
    auto fileName = "temp_ParallelFileWriter";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    auto data = GenerateRandomData(10 * 1024 * 1024);
    output << data;
    output.Flush();
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions().ThreadCount(5));
    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(data, read);
}

TEST(TParallelFileWriterTest, WriteFileSmallConcatenateBatchSize)
{
    TTestFixture fixture;
    auto fileName = "temp_ParallelFileWriter";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    auto data = GenerateRandomData(10 * 1024 * 1024);
    output << data;
    output.Flush();
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions().ThreadCount(5));
    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(data, read);
}

TEST(TParallelFileWriterTest, SpecifyTemporaryDirectory)
{
    TTestFixture fixture;
    auto fileName = "temp_ParallelFileWriter";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    auto data = GenerateRandomData(10 * 1024 * 1024);
    output << data;
    output.Flush();
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions()
        .ThreadCount(5)
        .TmpDirectory(fixture.GetWorkingDir()));
    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(data, read);
}

TEST(TParallelFileWriterTest, AppendFalse)
{
    TTestFixture fixture;
    auto fileName = "temp_ParallelFileWriter";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    auto data = GenerateRandomData(10 * 1024 * 1024);
    output << data;
    output.Flush();
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions().ThreadCount(5));
    {
        auto reader = client->CreateFileReader(path);
        auto read = reader->ReadAll();
        EXPECT_EQ(data, read);
    }
    WriteFileParallel(client, fileName, path.Append(false), TParallelFileWriterOptions().ThreadCount(5));
    {
        auto reader = client->CreateFileReader(path);
        auto read = reader->ReadAll();
        EXPECT_EQ(data, read);
    }
}

TEST(TParallelFileWriterTest, AppendTrue)
{
    TTestFixture fixture;
    auto fileName = "temp_ParallelFileWriter";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    auto data = GenerateRandomData(10 * 1024 * 1024);
    output << data;
    output.Flush();
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions().ThreadCount(5));
    {
        auto reader = client->CreateFileReader(path);
        auto read = reader->ReadAll();
        EXPECT_EQ(data, read);
    }
    WriteFileParallel(client, fileName, path.Append(true), TParallelFileWriterOptions().ThreadCount(5));
    {
        auto reader = client->CreateFileReader(path);
        auto read = reader->ReadAll();
        EXPECT_EQ(data + data, read);
    }
}

TEST(TParallelFileWriterTest,SmallData)
{
    TTestFixture fixture;
    auto fileName = "temp_ParallelFileWriter";
    TTempFileHandle file(fileName);
    TFileOutput output(file);
    TString data = "abcde";
    output << data;
    output.Flush();
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";
    WriteFileParallel(client, fileName, path, TParallelFileWriterOptions().ThreadCount(10));
    {
        auto reader = client->CreateFileReader(path);
        auto read = reader->ReadAll();
        EXPECT_EQ(data, read);
    }
    WriteFileParallel(client, fileName, path.Append(true), TParallelFileWriterOptions().ThreadCount(10));
    {
        auto reader = client->CreateFileReader(path);
        auto read = reader->ReadAll();
        EXPECT_EQ(data + data, read);
    }
}

TEST(TParallelFileWriterTest, StreamFiles)
{
    TTestFixture fixture;
    TString fileNameFirst = "temp_ParallelFileWriter_first";
    TString fileNameSecond = "temp_ParallelFileWriter_second";
    auto dataFirst = GenerateRandomData(5 * 1024 * 1024);
    auto dataSecond = GenerateRandomData(5 * 1024 * 1024);
    TTempFileHandle fileFirst(fileNameFirst);
    {
        TFileOutput output(fileFirst);
        output << dataFirst;
        output.Flush();
    }

    TTempFileHandle fileSecond(fileNameSecond);
    {
        TFileOutput output(fileSecond);
        output << dataSecond;
        output.Flush();
    }

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    auto writer = CreateParallelFileWriter(client, path, TParallelFileWriterOptions().ThreadCount(10));
    writer->WriteFile(fileNameFirst);
    writer->WriteFile(fileNameSecond);
    writer->Finish();

    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(dataFirst + dataSecond, read);
}

TEST(TParallelFileWriterTest, StreamBlobs)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    auto writer = CreateParallelFileWriter(client, path, TParallelFileWriterOptions().ThreadCount(10).MaxBlobSize(1024 * 1024));
    auto data = GenerateRandomData(5 * 1024 * 1024);

    for (size_t i = 0; i < 2; ++i) {
        writer->Write(NYT::TSharedRef::FromString(data));
    }
    writer->Finish();

    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(data + data, read);
}

TEST(TParallelFileWriterTest, FileAttributes)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    auto writer = CreateParallelFileWriter(
        client,
        path,
        TParallelFileWriterOptions()
            .FileAttributes(NYT::TNode::CreateMap()("custom_attr_name", 125))
    );
    auto data = GenerateRandomData(1024);

    writer->Write(NYT::TSharedRef::FromString(data));
    writer->Finish();

    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(data, read);
    EXPECT_EQ(client->Get(path.Path_ + "/@custom_attr_name").AsInt64(), 125);
}

TEST(TParallelFileWriterTest, MemoryLimitSingleProducer)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    auto writer = CreateParallelFileWriter(client, path,
        TParallelFileWriterOptions()
            .ThreadCount(10)
            .MaxBlobSize(1)
            .RamLimiter(::MakeIntrusive<TResourceLimiter>(static_cast<size_t>(2u))));

    TVector<TString> data;
    for (size_t i = 0; i < 15; ++i) {
        writer->Write(NYT::TSharedRef::FromString(ToString(i)));
        data.emplace_back(ToString(i));
    }
    writer->Finish();

    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(JoinStrings(data, ""), read);
}

TEST(TParallelFileWriterTest, MemoryLimitSingleProducerWithAcquireBufferSize)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    auto writer = CreateParallelFileWriter(client, path,
        TParallelFileWriterOptions()
            .ThreadCount(10)
            .MaxBlobSize(1)
            .AcquireRamForBuffers(true)
            .WriterOptions(TWriterOptions().DesiredChunkSize(1u))
            .RamLimiter(::MakeIntrusive<TResourceLimiter>(static_cast<size_t>(512'000'000u))));

    TVector<TString> data;
    for (size_t i = 0; i < 15; ++i) {
        writer->Write(NYT::TSharedRef::FromString(ToString(i)));
        data.emplace_back(ToString(i));
    }
    writer->Finish();

    auto reader = client->CreateFileReader(path);
    auto read = reader->ReadAll();
    EXPECT_EQ(JoinStrings(data, ""), read);
}

TEST(TParallelFileWriterTest, BadMemoryLimit)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(Y_UNUSED(::MakeIntrusive<TResourceLimiter>(0u)), yexception, "0");

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            CreateParallelFileWriter(client, path, TParallelFileWriterOptions()
                .MaxBlobSize(11)
                .RamLimiter(::MakeIntrusive<TResourceLimiter>(10u))),
            yexception,
            ""
    );
}

TEST(TParallelFileWriterTest, AutoFinish)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    {
        auto writer = CreateParallelFileWriter(client, path, TParallelFileWriterOptions().AutoFinish(false));
        writer->Write(TSharedRef::FromString("foo"));
    }

    EXPECT_EQ(client->Exists(path.Path_), false);
}
