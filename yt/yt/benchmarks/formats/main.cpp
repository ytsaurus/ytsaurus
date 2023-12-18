#include <yt/yt/library/formats/dsv_parser.h>
#include <yt/yt/library/formats/dsv_writer.h>
#include <yt/yt/library/formats/schemaful_dsv_writer.h>
#include <yt/yt/library/formats/yamr_parser.h>
#include <yt/yt/library/formats/yamr_writer.h>
#include <yt/yt/library/formats/yamred_dsv_parser.h>
#include <yt/yt/library/formats/yamred_dsv_writer.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/lexer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/stream.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>

using namespace NYT;
using namespace NYT::NFormats;
using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NJson;
using namespace NYT::NYTree;

static int ColumnCount = 40;
static int ValueLength = 1000;
static int RowCount = 10000;

TString GenerateString(int len)
{
    TString result;
    for (int j = 0; j < len; j++) {
        result.append('a' + std::rand() % 26);
    }
    return result;
}

TDsvFormatConfigPtr GetTskvConfig()
{
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";
    return config;
}

TYamrFormatConfigPtr GetYamrConfig()
{
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = false;
    return config;
}

TYamredDsvFormatConfigPtr GetYamredDsvConfig()
{
    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key_1");
    config->KeyColumnNames.push_back("key_2");
    config->SubkeyColumnNames.push_back("key_3");
    return config;
}

TJsonFormatConfigPtr GetJsonConfig()
{
    return New<TJsonFormatConfig>();
}

TTableSchemaPtr GetSchema()
{
    std::vector<TColumnSchema> columns;
    for (int i = 0; i < ColumnCount; ++i) {
        columns.emplace_back(
            Format("column_%v", i),
            EValueType::Int64);
    }
    return New<TTableSchema>(std::move(columns));
}

TSchemafulDsvFormatConfigPtr GetSchemafulDsvConfig()
{
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<TString>();
    auto schema = GetSchema();
    for (const auto& column : schema->Columns()) {
        config->Columns->push_back(column.Name());
    }
    return config;
}

void BenchmarkConsume(const std::vector<TString>& keys, const std::vector<TString>& values, NYson::IYsonConsumer* consumer)
{
    NProfiling::TWallTimer timer;
    {
        for (int recordIndex = 0; recordIndex < RowCount; ++recordIndex) {
            consumer->OnListItem();

            consumer->OnBeginMap();
            for (size_t row = 0; row < keys.size(); ++row) {
                consumer->OnKeyedItem(keys[row]);
                consumer->OnStringScalar(values[row]);
            }
            consumer->OnEndMap();
        }
    }
    Cerr << timer.GetElapsedTime().MicroSeconds() / 1e6 << Endl;
}

void BenchmarkSchemalessWrite(ISchemalessFormatWriterPtr writer)
{
    TUnversionedOwningRowBuilder builder;
    std::vector<TUnversionedOwningRow> owningRows(RowCount);
    std::vector<TUnversionedRow> rows(RowCount);

    Cerr << "Preparing" << Endl;

    for (int i = 0; i < RowCount; ++i) {
        for (int j = 0; j < ColumnCount; ++j) {
            TString value = GenerateString(ValueLength);
            builder.AddValue(MakeUnversionedStringValue(value, j));
        }
        owningRows[i] = builder.FinishRow();
        rows[i] = owningRows[i].Get();
    }

    Cerr << "Writing" << Endl;
    NProfiling::TWallTimer timer;
    {
        YT_VERIFY(writer->Write(rows));
    }
    writer->Close()
        .Get()
        .ThrowOnError();
    Cerr << timer.GetElapsedTime().MicroSeconds() / 1e6 << Endl;
}

void BenchmarkWrite(IUnversionedRowsetWriterPtr writer)
{
    Cerr << "Preparing" << Endl;

    std::vector<TUnversionedRow> rows(RowCount);
    TChunkedMemoryPool pool;

    for (int i = 0; i < RowCount; ++i) {
        auto row = TMutableUnversionedRow::Allocate(&pool, ColumnCount);
        for (int j = 0; j < ColumnCount; ++j) {
            row[j] = MakeUnversionedInt64Value(std::numeric_limits<i64>::max(), j);
        }
        rows[i] = row;
    }

    Cerr << "Writing" << Endl;

    NProfiling::TWallTimer timer;
    {
        YT_VERIFY(writer->Write(rows));
        YT_VERIFY(writer->Close().Get().IsOK());
    }
    Cerr << timer.GetElapsedTime().MicroSeconds() / 1e6 << Endl;
}

int main(int argc, char** argv)
{
    if (argc < 3 || argc > 6) {
        Cerr <<
            "Usage: " << argv[0] <<
            "<format: yamr, yson, tskv, json, schemaful_dsv, yamred_dsv> <action: read, write, schemaful_write, schemaless_write> "
            "[<value length> = 10] [<column count> = 10] [<record count> = 1e6]" << Endl;
        return 1;
    }

    if (argc >= 4) {
        ValueLength = FromString<int>(argv[3]);
    }
    if (argc >= 5) {
        ColumnCount = FromString<int>(argv[4]);
    }
    if (argc >= 6) {
        RowCount = FromString<int>(argv[5]);
    }

    std::string format(argv[1]);
    std::string action(argv[2]);
    if (action == "schemaless_write") {
        TNameTablePtr nameTable = New<TNameTable>();
        TStringStream outputStream;
        auto output = CreateAsyncAdapter(static_cast<IOutputStream*>(&outputStream));

        if (format == "yamr") {
            ColumnCount = 2;
            nameTable->RegisterName("key");
            nameTable->RegisterName("value");
            auto writer = CreateSchemalessWriterForYamr(
                New<TYamrFormatConfig>(),
                nameTable,
                output,
                false, // enableContextSaving
                New<TControlAttributesConfig>(),
                0); // keyColumnCount
            BenchmarkSchemalessWrite(writer);
        } else if (format == "dsv") {
            for (int i = 0; i < ColumnCount; ++i) {
                nameTable->RegisterName(GenerateString(ValueLength));
            }
            auto writer = CreateSchemalessWriterForDsv(
                New<TDsvFormatConfig>(),
                nameTable,
                output,
                false, // enableContextSaving
                New<TControlAttributesConfig>(),
                0); // keyColumnCount
            BenchmarkSchemalessWrite(writer);
        } else if (format == "yamred_dsv") {
            std::vector<TString> columnNames;
            for (int i = 0; i < ColumnCount; ++i) {
                columnNames.push_back(GenerateString(ValueLength));
                nameTable->RegisterName(columnNames.back());
            }
            TYamredDsvFormatConfigPtr defaultConfig = New<TYamredDsvFormatConfig>();
            defaultConfig->SetDefaults();
            defaultConfig->KeyColumnNames.push_back(columnNames[0]);
            defaultConfig->Lenval = true;
            auto writer = CreateSchemalessWriterForYamredDsv(
                defaultConfig,
                nameTable,
                output,
                false, // enableContextSaving
                New<TControlAttributesConfig>(),
                0); // keyColumnCount
            BenchmarkSchemalessWrite(writer);
        } else {
            Cerr << "Unsupported format: " << format << Endl;
            return 1;
        }
    } else if (action == "write") {
        // ToDo(psushin): migrate write test to schemaless_writer interface.
        std::vector<TString> keys;
        if (format == "yamr") {
            auto config = GetYamrConfig();
            keys.push_back(config->Value);
            keys.push_back(config->Subkey);
            keys.push_back(config->Key);
        } else {
            for (int i = 0; i < ColumnCount; ++i) {
                keys.push_back("key_" + ToString(i));
            }
        }

        std::vector<TString> values;
        for (size_t i = 0; i < keys.size(); ++i) {
            values.push_back(GenerateString(ValueLength));
        }

        TBufferedOutput output(&Cout, 1024 * 1024);
        if (format == "yson") {
            NYson::TYsonWriter consumer(&output, NYson::EYsonFormat::Binary, NYson::EYsonType::ListFragment);
            BenchmarkConsume(keys, values, &consumer);
        } else if (format == "json") {
            auto jsonConsumer = CreateJsonConsumer(&output, NYson::EYsonType::ListFragment, GetJsonConfig());
            BenchmarkConsume(keys, values, jsonConsumer.get());
            jsonConsumer->Flush();
        } else {
            Cerr << "Unsupported format: " << format << Endl;
            return 1;
        }
    } else if (action == "read") {
        NProfiling::TWallTimer timer;
        if (format == "yamr") {
            ParseYamr(&Cin, GetNullYsonConsumer(), GetYamrConfig());
        } else if (format == "tskv") {
            ParseDsv(&Cin, GetNullYsonConsumer(), GetTskvConfig());
        } else if (format == "yson") {
            ParseYson(TYsonInput(&Cin, NYson::EYsonType::ListFragment), GetNullYsonConsumer());
        } else if (format == "yamred_dsv") {
            ParseYamredDsv(&Cin, GetNullYsonConsumer(), GetYamredDsvConfig());
        } else if (format == "json") {
            ParseJson(&Cin, GetNullYsonConsumer(), GetJsonConfig(), NYson::EYsonType::ListFragment);
        } else {
            Cerr << "Unsupported format: " << format << Endl;
            return 1;
        }
        fprintf(stderr, "%.5f\n", timer.GetElapsedTime().MicroSeconds() / 1e6);
    } else if (action == "schemaful_write") {
        TBlobOutput syncOutput;
        auto asyncOutput = CreateAsyncAdapter(&syncOutput);
        if (format == "schemaful_dsv") {
            auto config = GetSchemafulDsvConfig();
            auto writer = CreateSchemafulWriterForSchemafulDsv(
                config,
                GetSchema(),
                asyncOutput);
            BenchmarkWrite(writer);
        } else {
            Cerr << "Unsupported format: " << format << Endl;
            return 1;
        }
    } else {
        Cerr << "Incorrect action: " << action << Endl;
        return 1;
    }

    return 0;
}

