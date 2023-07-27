#include <yt/cpp/mapreduce/library/parallel_io/parallel_file_writer.h>

#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(ParallelFileWriter) {
    void TryWriteFile(TParallelFileWriterOptions& options) {
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

    Y_UNIT_TEST(InvalidWriterOptionsFail) {
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

    Y_UNIT_TEST(JustWriteFile) {
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
    Y_UNIT_TEST(AppendFalse) {
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
    Y_UNIT_TEST(AppendTrue) {
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
    Y_UNIT_TEST(SmallData) {
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
}
