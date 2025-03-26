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

#include <yt/yt/library/arrow_adapter/arrow.h>

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

#include <contrib/libs/apache/orc/c++/include/orc/OrcFile.hh>

#include <sstream>

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
        data.push_back(row["int"].AsInt64());
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

template <typename T>
TString GenerateIntegerParquet(const std::vector<T>& data, const std::shared_ptr<arrow::DataType>& dataType)
{
    auto* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::Array> intArray;
    if constexpr (std::is_same<T, int16_t>::value) {
        arrow::Int16Builder intBuilder(pool);
        NArrow::ThrowOnError(intBuilder.AppendValues(data));
        NArrow::ThrowOnError(intBuilder.Finish(&intArray));
    } else if constexpr (std::is_same<T, int32_t>::value) {
        arrow::Int32Builder intBuilder(pool);
        NArrow::ThrowOnError(intBuilder.AppendValues(data));
        NArrow::ThrowOnError(intBuilder.Finish(&intArray));
    } else if constexpr (std::is_same<T, int64_t>::value) {
        arrow::Int64Builder intBuilder(pool);
        NArrow::ThrowOnError(intBuilder.AppendValues(data));
        NArrow::ThrowOnError(intBuilder.Finish(&intArray));
    } else {
        YT_ABORT();
    }

    auto schema = arrow::schema({arrow::field("int", dataType, false)});
    auto table = arrow::Table::Make(schema, {intArray});

    auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    NArrow::ThrowOnError(parquet::arrow::WriteTable(*table, pool, outputStream, ChunkSize));
    auto buffer = outputStream->Finish().ValueOrDie();
    return TString(buffer->ToString());
}

class StringOutputStream
    : public orc::OutputStream
{
public:
    uint64_t getLength() const override
    {
        return Length_;
    }

    uint64_t getNaturalWriteSize() const override
    {
        return 4 * 1024;
    }

    void write(const void* buf, size_t length) override
    {
        Length_ += length;
        Stream_.write(static_cast<const char*>(buf), length);
    }

    std::string str()
    {
        return Stream_.str();
    }

    const std::string& getName() const override
    {
        return Name_;
    }

    void close() override
    { }

private:
    std::ostringstream Stream_;
    const std::string Name_ = "StringOutputStream";
    int Length_ = 0;
};

std::string GenerateIntegerORC(const std::vector<i64>& intArray) {
    orc::WriterOptions options;
    std::unique_ptr<orc::Type> schema(orc::Type::buildTypeFromString("struct<int:int>"));
    auto outStream = std::make_unique<StringOutputStream>();
    std::unique_ptr<orc::Writer> writer = orc::createWriter(*schema, outStream.get(), options);

    std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(intArray.size());
    orc::StructVectorBatch* structBatch = static_cast<orc::StructVectorBatch*>(batch.get());
    orc::LongVectorBatch* intField = static_cast<orc::LongVectorBatch*>(structBatch->fields[0]);

    structBatch->numElements = intArray.size();
    intField->resize(intArray.size());
    for (size_t i = 0; i < intArray.size(); i++) {
        intField->data[i] = intArray[i];
    }

    writer->add(*batch);
    writer->close();

    return outStream->str();
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
    arrow::Int64Builder intBuilder(pool);
    NArrow::ThrowOnError(intBuilder.AppendValues(data.IntegerData));
    std::shared_ptr<arrow::Array> intArray;
    NArrow::ThrowOnError(intBuilder.Finish(&intArray));

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
        arrow::field("int", arrow::int64()),
        arrow::field("double", arrow::float64()),
        arrow::field("bool", arrow::boolean()),
        arrow::field("string", arrow::binary())
    });

    auto table = arrow::Table::Make(schema, {intArray, doubleArray, booleanArray, binaryArray});
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

    virtual std::vector<TString> GenerateFormatData() = 0;
};

class TDifferentSchemaParquetGenerator
    : public IParquetGenerator
{
public:
    ~TDifferentSchemaParquetGenerator() = default;

    void VerifyAnswer(const NYT::IClientPtr& client, const TString& resultTable) override
    {
        auto tableData = ReadIntegersFromTable(client, resultTable);
        EXPECT_EQ(tableData, ResultData_);
    }

    std::vector<TString> GenerateFileNames() override
    {
        std::vector<TString> fileNames;
        for (int fileIndex = 0; fileIndex < FileCount_; fileIndex++) {
            fileNames.emplace_back(ToString(fileIndex) + ".parquet");
        }
        return fileNames;
    }

    std::vector<TString> GenerateFormatData() override
    {
        std::vector<TString> formatData;

        std::vector<i16> firstFileData;
        std::vector<i32> secondFileData;
        std::vector<i64> thirdFileData;

        for (int elemIndex = 0; elemIndex < ElementCount_; elemIndex++) {
            firstFileData.push_back(rand());
            secondFileData.push_back(rand());
            thirdFileData.push_back(rand());
        }

        formatData.push_back(GenerateIntegerParquet(firstFileData, arrow::int16()));
        formatData.push_back(GenerateIntegerParquet(secondFileData, arrow::int32()));
        formatData.push_back(GenerateIntegerParquet(thirdFileData, arrow::int64()));

        AddData(firstFileData);
        AddData(secondFileData);
        AddData(thirdFileData);

        return formatData;
    }

private:
    static constexpr int FileCount_ = 3;
    static constexpr int ElementCount_ = 10;

    std::vector<i64> ResultData_;

    template <typename T>
    void AddData(const std::vector<T>& dataFile)
    {
        for (const auto& value : dataFile) {
            ResultData_.push_back(value);
        }
    }
};

class TSmallParquetGenerator
    : public IParquetGenerator
{
public:
    explicit TSmallParquetGenerator(EFileFormat format)
        : Format_(format)
    { }

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
            switch (Format_) {
                case EFileFormat::Parquet:
                    fileNames.emplace_back(ToString(fileIndex) + ".parquet");
                    break;
                case EFileFormat::ORC:
                    fileNames.emplace_back(ToString(fileIndex) + ".orc");
                    break;
            }
        }
        return fileNames;
    }

    std::vector<TString> GenerateFormatData() override
    {
        std::vector<TString> formatData;
        for (int fileIndex = 0; fileIndex < FileCount_; fileIndex++) {
            std::vector<i64> fileData;
            for (int elemIndex = 0; elemIndex < ElementCount_; elemIndex++) {
                fileData.push_back(rand());
            }
            AddData(fileData);
            switch (Format_) {
                case EFileFormat::Parquet:
                    formatData.push_back(GenerateIntegerParquet(fileData, arrow::int64()));
                    break;
                case EFileFormat::ORC:
                    formatData.push_back(GenerateIntegerORC(fileData));
                    break;
            }
        }
        return formatData;
    }

private:
    static constexpr int FileCount_ = 3;
    static constexpr int ElementCount_ = 10;

    const EFileFormat Format_;

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
        EXPECT_EQ(tableData.size(), ResultData_.IntegerData.size());
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
            fileNames.emplace_back(ToString(fileIndex) + ".parquet");
        }
        return fileNames;
    }

    std::vector<TString> GenerateFormatData() override
    {
        std::vector<TString> formatData;
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
            formatData.push_back(GenerateMultiTypesParquet(fileData));
        }
        return formatData;
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
        return {"0.parquet"};
    }

    std::vector<TString> GenerateFormatData() override
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
        auto formatData = Generator->GenerateFormatData();
        Server->AddHandler(path, New<THuggingfaceListOfFilesHttpHandler>(TestUrl, parquetFilesUrls));

        for (int fileIndex = 0; fileIndex < std::ssize(formatData); fileIndex++) {
            auto downloaderPath = NYT::Format("/%v", parquetFilesUrls[fileIndex]);
            Server->AddHandler(downloaderPath, New<THuggingfaceDownloaderFilesHttpHandler>(std::move(formatData[fileIndex])));
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
        Generator = std::make_shared<TSmallParquetGenerator>(EFileFormat::Parquet);
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

    NTools::NImporter::ImportFilesFromHuggingface(
        proxy,
        Dataset,
        /*subset*/ "default",
        Split,
        resultTable,
        EFileFormat::Parquet,
        TestUrl);

    TTableSchema schema;
    Deserialize(schema, client->Get(resultTable + "/@schema"));

    EXPECT_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema().Name("int").Type(NTi::Int64()))
    );

    Generator->VerifyAnswer(client, resultTable);
}

class TSmallOrcHuggingfaceServerTest
    : public THttpHuggingfaceServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TSmallParquetGenerator>(EFileFormat::ORC);
    }
};

TEST_F(TSmallOrcHuggingfaceServerTest, SimpleImportORCFilesFromHuggingface)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportFilesFromHuggingface(
        proxy,
        Dataset,
        /*subset*/ "default",
        Split,
        resultTable,
        EFileFormat::ORC,
        TestUrl);

    TTableSchema schema;
    Deserialize(schema, client->Get(resultTable + "/@schema"));

    EXPECT_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema().Name("int").Type(ToTypeV3(EValueType::VT_INT32, false)))
    );

    Generator->VerifyAnswer(client, resultTable);
}

class TDifferentSchemasHuggingfaceServerTest
    : public THttpHuggingfaceServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TDifferentSchemaParquetGenerator>();
    }
};

TEST_F(TDifferentSchemasHuggingfaceServerTest, DifferentSchemas)
{
    NTesting::TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString resultTable = workingDir + "/result_table";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    client->Set("//sys/controller_agents/config/enable_merge_schemas_during_schema_infer", true);

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportFilesFromHuggingface(
        proxy,
        Dataset,
        /*subset*/ "default",
        Split,
        resultTable,
        EFileFormat::Parquet,
        TestUrl);

    Generator->VerifyAnswer(client, resultTable);
}


class TBigHuggingfaceServerTest
    : public THttpHuggingfaceServerTestBase
{
private:
    void InitializeGenerator() override
    {
        Generator = std::make_shared<TSmallParquetGenerator>(EFileFormat::Parquet);
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

    NTools::NImporter::ImportFilesFromHuggingface(
        proxy,
        Dataset,
        /*subset*/ "default",
        Split,
        resultTable,
        EFileFormat::Parquet,
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
        auto formatData = Generator->GenerateFormatData();
        for (int fileIndex = 0; fileIndex < std::ssize(fileNames); fileIndex++) {
            WaitFor(client->PutObject({
                .Bucket = Bucket,
                .Key = "parquet/" + fileNames[fileIndex],
                .Data = TSharedRef::FromString(formatData[fileIndex])
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
        Generator = std::make_shared<TSmallParquetGenerator>(EFileFormat::Parquet);
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

    NTools::NImporter::ImportFilesFromS3(
        proxy,
        TestUrl,
        /*region*/ "",
        Bucket,
        /*prefix*/ "parquet",
        resultTable,
        EFileFormat::Parquet);

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
    TString resultTable = workingDir + "/result_table2";
    TConfig::Get()->RemoteTempTablesDirectory = workingDir + "/table_storage";

    TString proxy = GetEnv("YT_PROXY");

    NTools::NImporter::ImportFilesFromS3(
        proxy,
        TestUrl,
        /*region*/ "",
        Bucket,
        /*prefix*/ "parquet",
        resultTable,
        EFileFormat::Parquet);

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
        NTools::NImporter::ImportFilesFromS3(
            proxy,
            TestUrl,
            /*region*/ "",
            Bucket,
            /*prefix*/ "parquet",
            resultTable,
            EFileFormat::Parquet),
        std::exception,
        "this is not a parquet file");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTools::NImporter
