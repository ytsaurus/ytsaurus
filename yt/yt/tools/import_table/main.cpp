#include <yt/yt/tools/import_table/lib/import_table.h>
#include <yt/yt/tools/import_table/lib/config.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/cpp/mapreduce/interface/init.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/getopt/modchooser.h>

namespace NYT::NTools::NImporter {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TOpts
{
    TOpts()
        : Opts(NLastGetopt::TOpts::Default())
    {
        Opts.AddLongOption("proxy", "Specify cluster to run command")
            .StoreResult(&Proxy)
            .Required();
        Opts.AddLongOption("output", "Path to output table")
            .StoreResult(&ResultTable)
            .Required();
        Opts.AddLongOption("format", "Format of files")
            .DefaultValue("parquet")
            .StoreResult(&Format);
        Opts.AddLongOption("config", "YSON file with configuration for fine-grained tuning")
            .Optional()
            .StoreResult(&Config);
    }

    NLastGetopt::TOpts Opts;

    TString Proxy;
    TString ResultTable;
    TString Format;
    std::optional<TString> Config;
};

struct TOptsHuggingface
    : public TOpts
{
    TOptsHuggingface()
        : TOpts()
    {
        Opts.AddLongOption("dataset", "Name of dataset")
            .StoreResult(&Dataset)
            .Required();
        Opts.AddLongOption("subset", "Name of subset")
            .DefaultValue("default")
            .StoreResult(&Subset);
        Opts.AddLongOption("split", "Name of split")
            .StoreResult(&Split)
            .Required();
    }

    TString Dataset;
    TString Subset;
    TString Split;
};

struct TOptsS3
    : public TOpts
{
    TOptsS3()
        : TOpts()
    {
        Opts.AddLongOption("url", "Endpoint URL of S3 storage")
            .StoreResult(&Url)
            .Required();
        Opts.AddLongOption("region", "Region")
            .DefaultValue("")
            .StoreResult(&Region);
        Opts.AddLongOption("bucket", "Name of bucket in S3")
            .StoreResult(&Bucket)
            .Required();
        Opts.AddLongOption("prefix", "Common prefix of target files")
            .DefaultValue("")
            .StoreResult(&Prefix);
    }

    TString Url;
    TString Region;
    TString Bucket;
    TString Prefix;
};

////////////////////////////////////////////////////////////////////////////////

TImportConfigPtr LoadConfig(const std::optional<TString>& configPath)
{
    auto config = !configPath
        ? New<TImportConfig>()
        : ConvertTo<TImportConfigPtr>(TYsonString(TUnbufferedFileInput(*configPath).ReadAll()));
    ConfigureSingletons(config->Singletons);
    return config;
}

int ImportFilesFromS3(int argc, const char** argv)
{
    TOptsS3 opts;
    NLastGetopt::TOptsParseResult parseResult(&opts.Opts, argc, argv);

    if (opts.Format == "parquet") {
        ImportFilesFromS3(
            opts.Proxy,
            opts.Url,
            opts.Region,
            opts.Bucket,
            opts.Prefix,
            opts.ResultTable,
            EFileFormat::Parquet,
            LoadConfig(opts.Config));
    } else if (opts.Format == "orc") {
         ImportFilesFromS3(
            opts.Proxy,
            opts.Url,
            opts.Region,
            opts.Bucket,
            opts.Prefix,
            opts.ResultTable,
            EFileFormat::ORC,
            LoadConfig(opts.Config));
    } else {
        THROW_ERROR_EXCEPTION("Unsupported format, Parquet and ORC are supported now");
    }

    return 0;
}

int ImportFilesFromHuggingface(int argc, const char** argv)
{
    TOptsHuggingface opts;
    NLastGetopt::TOptsParseResult parseResult(&opts.Opts, argc, argv);

    if (opts.Format == "parquet") {
        ImportFilesFromHuggingface(
            opts.Proxy,
            opts.Dataset,
            opts.Subset,
            opts.Split,
            opts.ResultTable,
             EFileFormat::Parquet,
            /*urlOverride*/ std::nullopt,
            LoadConfig(opts.Config));
    } else  if (opts.Format == "orc") {
        ImportFilesFromHuggingface(
            opts.Proxy,
            opts.Dataset,
            opts.Subset,
            opts.Split,
            opts.ResultTable,
            EFileFormat::ORC,
            /*urlOverride*/ std::nullopt,
            LoadConfig(opts.Config));
    } else {
        THROW_ERROR_EXCEPTION("Unsupported format, Parquet and ORC are supported now");
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

void ImportFiles(int argc, const char** argv)
{
    TModChooser modChooser;

    modChooser.AddMode(
        "huggingface",
        ImportFilesFromHuggingface,
        "-- import files from huggingface");
    modChooser.AddMode(
        "s3",
        ImportFilesFromS3,
        "-- import files from S3");

    modChooser.Run(argc, argv);
}

} // namespace NYT::NTools::NImporter

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::Initialize();
    try {
        NYT::NTools::NImporter::ImportFiles(argc, argv);
    } catch (const std::exception& e) {
        Cerr << ToString(NYT::TError(e));
        return 1;
    }

    return 0;
}
