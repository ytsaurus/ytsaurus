#include <yt/cpp/mapreduce/library/parallel_io/parallel_file_writer.h>
#include <yt/cpp/mapreduce/library/parallel_io/resource_limiter.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/vector.h>

#include <util/system/tempfile.h>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(ParallelFileWriter)
{
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

    Y_UNIT_TEST(InvalidWriterOptionsFail)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TryWriteFile(TParallelFileWriterOptions().WriterOptions(
                TWriterOptions()
                    .UploadReplicationFactor(0))),
            NYT::TErrorResponse,
            "/upload_replication_factor");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TryWriteFile(TParallelFileWriterOptions().WriterOptions(
                TWriterOptions()
                    .MinUploadReplicationFactor(0))),
            NYT::TErrorResponse,
            "/min_upload_replication_factor");
    }

    Y_UNIT_TEST(JustWriteFile)
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
        UNIT_ASSERT_EQUAL(data, read);
    }

    Y_UNIT_TEST(SpecifyTemporaryDirectory)
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
        UNIT_ASSERT_EQUAL(data, read);
    }

    Y_UNIT_TEST(AppendFalse)
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
            UNIT_ASSERT_EQUAL(data, read);
        }
        WriteFileParallel(client, fileName, path.Append(false), TParallelFileWriterOptions().ThreadCount(5));
        {
            auto reader = client->CreateFileReader(path);
            auto read = reader->ReadAll();
            UNIT_ASSERT_EQUAL(data, read);
        }
    }

    Y_UNIT_TEST(AppendTrue)
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
            UNIT_ASSERT_EQUAL(data, read);
        }
        WriteFileParallel(client, fileName, path.Append(true), TParallelFileWriterOptions().ThreadCount(5));
        {
            auto reader = client->CreateFileReader(path);
            auto read = reader->ReadAll();
            UNIT_ASSERT_EQUAL(data + data, read);
        }
    }

    Y_UNIT_TEST(SmallData)
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
            UNIT_ASSERT_EQUAL(data, read);
        }
        WriteFileParallel(client, fileName, path.Append(true), TParallelFileWriterOptions().ThreadCount(10));
        {
            auto reader = client->CreateFileReader(path);
            auto read = reader->ReadAll();
            UNIT_ASSERT_EQUAL(data + data, read);
        }
    }

    Y_UNIT_TEST(StreamFiles)
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
        UNIT_ASSERT_EQUAL(dataFirst + dataSecond, read);
    }

    Y_UNIT_TEST(StreamBlobs)
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
        UNIT_ASSERT_EQUAL(data + data, read);
    }

    Y_UNIT_TEST(MemoryLimitSingleProducer)
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
        UNIT_ASSERT_EQUAL(JoinStrings(data, ""), read);
    }

    Y_UNIT_TEST(BadMemoryLimit)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(::MakeIntrusive<TResourceLimiter>(0u), yexception, "0");

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        TRichYPath path = workingDir + "/file";

        UNIT_ASSERT_EXCEPTION_CONTAINS(
                CreateParallelFileWriter(client, path, TParallelFileWriterOptions()
                    .MaxBlobSize(11)
                    .RamLimiter(::MakeIntrusive<TResourceLimiter>(10u))),
                yexception,
                ""
        );
    }
}
