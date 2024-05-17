#include <yt/yt/core/test_framework/test_key.h>

#include <yt/yt/tools/import_table/lib/import_table.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/connection_pool.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>
#include <yt/yt/core/http/stream.h>

#include <yt/yt/core/https/config.h>
#include <yt/yt/core/https/server.h>

#include <yt/yt/library/arrow_parquet_adapter/arrow.h>

#include <yt/yt/library/huggingface_client/client.h>

#include <yt/yt/library/s3/client.h>

#include <yt/yt/core/misc/error.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/json/json_writer.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node.h>

#include <util/system/env.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT::NTools::NImporter {
namespace {

using namespace NConcurrency;
using namespace NCrypto;
using namespace NHttp;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

constexpr int ChunkSize = 64_KB;

////////////////////////////////////////////////////////////////////////////////

TString GenerateString(int length)
{
    TString result;
    for (int j = 0; j < length; j++) {
        result.append('a' + std::rand() % 26);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<int64_t> ReadIntegersFromTable(const NYT::IClientPtr& client, const TString& tablePath)
{
    auto reader = client->CreateTableReader<TNode>(tablePath);

    std::vector<int64_t> data;
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        data.push_back(row["int64"].AsInt64());
    }
    return data;
}

std::vector<bool> ReadBooleanFromTable(const NYT::IClientPtr& client, const TString& tablePath)
{
    auto reader = client->CreateTableReader<TNode>(tablePath);

    std::vector<bool> data;
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        data.push_back(row["bool"].AsBool());
    }
    return data;
}

std::vector<double> ReadDoubleFromTable(const NYT::IClientPtr& client, const TString& tablePath)
{
    auto reader = client->CreateTableReader<TNode>(tablePath);

    std::vector<double> data;
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        data.push_back(row["double"].AsDouble());
    }
    return data;
}

std::vector<std::string> ReadStringFromTable(const NYT::IClientPtr& client, const TString& tablePath)
{
    auto reader = client->CreateTableReader<TNode>(tablePath);

    std::vector<std::string> data;
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        data.push_back(row["string"].AsString());
    }
    return data;
}

////////////////////////////////////////////////////////////////////////////////

TString GenerateIntegerParquet(const std::vector<int64_t>& data)
{
    auto* pool = arrow::default_memory_pool();
    arrow::Int64Builder i64builder(pool);
    NArrow::ThrowOnError(i64builder.AppendValues(data));
    std::shared_ptr<arrow::Array> i64array;
    NArrow::ThrowOnError(i64builder.Finish(&i64array));

    auto schema = arrow::schema({arrow::field("int64", arrow::int64())});
    auto table = arrow::Table::Make(schema, {i64array});

    auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    NArrow::ThrowOnError(parquet::arrow::WriteTable(*table, pool, outputStream, ChunkSize));
    auto buffer = outputStream->Finish().ValueOrDie();
    return TString(buffer->ToString());
}

struct MultiTypeData
{
    std::vector<int64_t> IntegerData;
    std::vector<double> FloatData;
    std::vector<bool> BooleanData;
    std::vector<std::string> StringData;
};

TString GenerateMultiTypesParquet(const MultiTypeData& data)
{
    auto* pool = arrow::default_memory_pool();

    // Build integer data.
    arrow::Int64Builder i64builder(pool);
    NArrow::ThrowOnError(i64builder.AppendValues(data.IntegerData));
    std::shared_ptr<arrow::Array> i64Array;
    NArrow::ThrowOnError(i64builder.Finish(&i64Array));

    // Build double data.
    arrow::DoubleBuilder doubleBuilder(pool);
    NArrow::ThrowOnError(doubleBuilder.AppendValues(data.FloatData));
    std::shared_ptr<arrow::Array> doubleArray;
    NArrow::ThrowOnError(doubleBuilder.Finish(&doubleArray));

    // Build boolean data.
    arrow::BooleanBuilder booleanBuilder(pool);
    NArrow::ThrowOnError(booleanBuilder.AppendValues(data.BooleanData));
    std::shared_ptr<arrow::Array> booleanArray;
    NArrow::ThrowOnError(booleanBuilder.Finish(&booleanArray));

    // Build string data.
    arrow::BinaryBuilder binaryBuilder(pool);
    NArrow::ThrowOnError(binaryBuilder.AppendValues(data.StringData));
    std::shared_ptr<arrow::Array> binaryArray;
    NArrow::ThrowOnError(binaryBuilder.Finish(&binaryArray));

    auto schema = arrow::schema({
        arrow::field("int64", arrow::int64()),
        arrow::field("double", arrow::float64()),
        arrow::field("bool", arrow::boolean()),
        arrow::field("string", arrow::binary())
    });

    auto table = arrow::Table::Make(schema, {i64Array, doubleArray, booleanArray, binaryArray});
    auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    NArrow::ThrowOnError(parquet::arrow::WriteTable(*table, pool, outputStream, ChunkSize));
    auto buffer = outputStream->Finish().ValueOrDie();
    return TString(buffer->ToString());
}

class IParquetGenerator
{
public:
    virtual ~IParquetGenerator() = default;

    virtual void VerifyAnswer(const NYT::IClientPtr& client, const TString& resultTable) = 0;

    virtual std::vector<TString> GenerateFileNames() = 0;

    virtual std::vector<TString> GenerateParquetData() = 0;
};

class TSmallParquetGenerator
    : public IParquetGenerator
{
public:
    ~TSmallParquetGenerator() = default;

    void VerifyAnswer(const NYT::IClientPtr& client, const TString& resultTable) override
    {
        auto tableData = ReadIntegersFromTable(client, resultTable);
        EXPECT_EQ(tableData, ResultData_);
    }

    std::vector<TString> GenerateFileNames() override
    {
        std::vector<TString> fileNames;
        for (int fileIndex = 0; fileIndex < FileCount_; fileIndex++) {
            fileNames.emplace_back(ToString(fileIndex) + ".par");
        }
        return fileNames;
    }

    std::vector<TString> GenerateParquetData() override
    {
        std::vector<TString> parquetData;
        for (int fileIndex = 0; fileIndex < FileCount_; fileIndex++) {
            std::vector<i64> fileData;
            for (int elemIndex = 0; elemIndex < ElementCount_; elemIndex++) {
                fileData.push_back(rand());
            }
            AddData(fileData);
            parquetData.push_back(GenerateIntegerParquet(fileData));
        }
        return parquetData;
    }

private:
    static constexpr int FileCount_ = 3;
    static constexpr int ElementCount_ = 10;

    std::vector<i64> ResultData_;

    void AddData(const std::vector<i64>& dataFile)
    {
        for (const auto& value : dataFile) {
            ResultData_.push_back(value);
        }
    }
};

class TBigParquetGenerator
    : public IParquetGenerator
{
public:
    ~TBigParquetGenerator() = default;

    void VerifyAnswer(const NYT::IClientPtr& client, const TString& resultTable) override
    {
        auto tableData = ReadIntegersFromTable(client, resultTable);
        EXPECT_EQ(tableData, ResultData_.IntegerData);

        auto boolData = ReadBooleanFromTable(client, resultTable);
        EXPECT_EQ(boolData, ResultData_.BooleanData);

        auto doubleData = ReadDoubleFromTable(client, resultTable);
        EXPECT_EQ(doubleData, ResultData_.FloatData);

        auto stringData = ReadStringFromTable(client, resultTable);
        EXPECT_EQ(stringData, ResultData_.StringData);
    }

    std::vector<TString> GenerateFileNames() override
    {
        std::vector<TString> fileNames;
        for (int fileIndex = 0; fileIndex < FileCount_; fileIndex++) {
            fileNames.emplace_back(ToString(fileIndex) + ".par");
        }
        return fileNames;
    }

    std::vector<TString> GenerateParquetData() override
    {
        std::vector<TString> parquetData;
        for (int fileIndex = 0; fileIndex < FileCount_; fileIndex++) {
            MultiTypeData fileData;
            for (int elemIndex = 0; elemIndex < ElementCount_; elemIndex++) {
                i64 intValue = rand();
                double doubleValue = 3.14;
                bool boolValue = ((rand() % 2) == 0);
                TString stringValue = GenerateString(StringLength_);
                AddData(fileData, intValue, doubleValue, boolValue, stringValue);
                AddData(ResultData_, intValue, doubleValue, boolValue, stringValue);
            }
            parquetData.push_back(GenerateMultiTypesParquet(fileData));
        }
        return parquetData;
    }

private:
    static constexpr int FileCount_ = 3;
    static constexpr int ElementCount_ = 100000;
    static constexpr int StringLength_ = 6;

    MultiTypeData ResultData_;

    void AddData(
        MultiTypeData& data,
        i64 intValue,
        double doubleValue,
        bool boolValue,
        const TString& stringValue)
    {
        data.IntegerData.push_back(intValue);
        data.FloatData.push_back(doubleValue);
        data.BooleanData.push_back(boolValue);
        data.StringData.push_back(stringValue);
    }
};

class TWrongParquetGenerator
    : public IParquetGenerator
{
public:
    ~TWrongParquetGenerator() = default;

     void VerifyAnswer(const NYT::IClientPtr& /*client*/, const TString& /*resultTable*/) override
    {
        YT_ABORT();
    }

    std::vector<TString> GenerateFileNames() override
    {
        return {"0.par"};
    }

    std::vector<TString> GenerateParquetData() override
    {
        char data[8];
        int metadataSize = 1e9;
        memcpy(data, &metadataSize, 4);
        return {TString(data, 8)};
    }
};

////////////////////////////////////////////////////////////////////////////////

class THuggingfaceListOfFilesHttpHandler
    : public IHttpHandler
{
public:
    THuggingfaceListOfFilesHttpHandler(const TString& testUrl, const std::vector<TString>& parquetFilesUrls)
        : TestUrl_(testUrl)
        , ParquetFilesUrls_(parquetFilesUrls)
    { }

    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);

        TStringStream out;
        NJson::TJsonWriter json(&out, false);
        json.OpenArray();

        for (const auto& val : ParquetFilesUrls_) {
            json.Write(TestUrl_ + "/" + val);
        }
        json.CloseArray();
        json.Flush();

        WaitFor(rsp->Write(TSharedRef::FromString(out.Str()))).ThrowOnError();

        WaitFor(rsp->Close()).ThrowOnError();
    }

private:
    TString TestUrl_;
    std::vector<TString> ParquetFilesUrls_;
};

class THuggingfaceDownloaderFilesHttpHandler
    : public IHttpHandler
{
public:
    THuggingfaceDownloaderFilesHttpHandler(TString data)
        : Data_(std::move(data))
    { }

    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        auto sharedData = TSharedRef::FromString(Data_);
        for (i64 startBatchIndex = 0; startBatchIndex < std::ssize(Data_); startBatchIndex += BatchSize_) {
            WaitFor(rsp->Write(sharedData.Slice(startBatchIndex, std::min(std::ssize(Data_), startBatchIndex + BatchSize_))))
                .ThrowOnError();
        }

        WaitFor(rsp->Close()).ThrowOnError();
    }

private:
    const i64 BatchSize_ = 1024;

    TString Data_;
};

////////////////////////////////////////////////////////////////////////////////

class THttpHuggingfaceServerTestBase
    : public ::testing::Test
{
protected:
    const TString Dataset = "TestDataset";
    const TString Split =  "train";

    IPollerPtr Poller;
    IServerPtr Server;
    std::shared_ptr<IParquetGenerator> Generator;
    ::NTesting::TPortHolder TestPort;
    TString TestUrl;

private:
    virtual void  InitializeGenerator() = 0;

    void SetupServer(const NHttp::TServerConfigPtr& config)
    {
        config->Port = TestPort;
    }

    void SetUp() override
    {
        InitializeGenerator();
        TestPort = ::NTesting::GetFreePort();
        TestUrl = Format("http://localhost:%v", TestPort);
        Poller = CreateThreadPoolPoller(4, "HttpTest");

        auto serverConfig = New<NHttps::TServerConfig>();
        serverConfig->Credentials = New<NHttps::TServerCredentialsConfig>();
        serverConfig->Credentials->PrivateKey = New<TPemBlobConfig>();
        serverConfig->Credentials->PrivateKey->Value = TestCertificate;
        serverConfig->Credentials->CertChain = New<TPemBlobConfig>();
        serverConfig->Credentials->CertChain->Value = TestCertificate;
        SetupServer(serverConfig);
        Server = NHttps::CreateServer(serverConfig, Poller);

        auto path = NYT::Format("/api/datasets/%v/parquet/%v/%v", Dataset, "default", Split);
        auto parquetFilesUrls = Generator->GenerateFileNames();
        auto parquetData = Generator->GenerateParquetData();
        Server->AddHandler(path, New<THuggingfaceListOfFilesHttpHandler>(TestUrl, parquetFilesUrls));

        for (int fileIndex = 0; fileIndex < std::ssize(parquetData); fileIndex++) {
            auto downloaderPath = NYT::Format("/%v", parquetFilesUrls[fileIndex]);
            Server->AddHandler(downloaderPath, New<THuggingfaceDownloaderFilesHttpHandler>(std::move(parquetData[fileIndex])));
        }

        Server->Start();
    }

    void TearDown() override
    {
        Server->Stop();
        Server.Reset();
        Poller->Shutdown();
        Poller.Reset();
        TestPort.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSmallHuggingfaceServerTest
    : public THttpHuggingfaceServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TSmallParquetGenerator>();
    }
};

TEST_F(TSmallHuggingfaceServerTest, SimpleImportTableFromHuggingface)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportParquetFilesFromHuggingface(
        proxy,
        Dataset,
        "default",
        Split,
        resultTable,
        TestUrl);

    Generator->VerifyAnswer(client, resultTable);
}

class TBigHuggingfaceServerTest
    : public THttpHuggingfaceServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TSmallParquetGenerator>();
    }
};

TEST_F(TBigHuggingfaceServerTest, ImportBigTableFromHuggingface)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportParquetFilesFromHuggingface(
        proxy,
        Dataset,
        "default",
        Split,
        resultTable,
        TestUrl);

    Generator->VerifyAnswer(client, resultTable);
}

////////////////////////////////////////////////////////////////////////////////

class T3ServerTestBase
    : public ::testing::Test
{
protected:
    const TString Bucket = "bucket";

    NS3::IClientPtr S3Client;
    std::shared_ptr<IParquetGenerator> Generator;
    TString TestUrl;

private:
    virtual void  InitializeGenerator() = 0;

    void SetUp() override
    {
         InitializeGenerator();

        auto clientConfig = New<NS3::TS3ClientConfig>();

        TestUrl = "http://127.0.0.1:" + GetEnv("S3MDS_PORT");
        clientConfig->Url = TestUrl;
        clientConfig->Bucket = Bucket;

        auto poller = CreateThreadPoolPoller(1, "S3TestPoller");
        auto client = NS3::CreateClient(
            std::move(clientConfig),
            poller,
            poller->GetInvoker());

        WaitFor(client->Start())
            .ThrowOnError();

        WaitFor(client->PutBucket({
            .Bucket = Bucket,
        })).ValueOrThrow();

        auto fileNames = Generator->GenerateFileNames();
        auto parquetData = Generator->GenerateParquetData();
        for (int fileIndex = 0; fileIndex < std::ssize(fileNames); fileIndex++) {
            WaitFor(client->PutObject({
                .Bucket = Bucket,
                .Key = "parquet/" + fileNames[fileIndex],
                .Data = TSharedRef::FromString(parquetData[fileIndex])
            })).ValueOrThrow();
        }
    }
};

class TSmallS3ServerTest
    : public T3ServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TSmallParquetGenerator>();
    }
};

TEST_F(TSmallS3ServerTest, SimpleImportTableFromS3)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportParquetFilesFromS3(
        proxy,
        TestUrl,
        /*region*/ "",
        Bucket,
        /*prefix*/ "parquet",
        resultTable);

    Generator->VerifyAnswer(client, resultTable);
}

class TBigS3ServerTest
    : public T3ServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TBigParquetGenerator>();
    }
};

TEST_F(TBigS3ServerTest, ImportBigTableFromS3)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportParquetFilesFromS3(
        proxy,
        TestUrl,
        /*region*/ "",
        Bucket,
        /*prefix*/ "parquet",
        resultTable);

    Generator->VerifyAnswer(client, resultTable);
}

////////////////////////////////////////////////////////////////////////////////

class TWrongServerTest
    : public T3ServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TWrongParquetGenerator>();
    }
};

TEST_F(TWrongServerTest, ExceptionThrown)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        NTools::NImporter::ImportParquetFilesFromS3(
            proxy,
            TestUrl,
            /*region*/ "",
            Bucket,
            /*prefix*/ "parquet",
            resultTable),
        std::exception,
        "Metadata size of Parquet file is too big");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTools::NImporter
