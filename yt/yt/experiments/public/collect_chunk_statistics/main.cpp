#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/stream/output.h>
#include <util/generic/algorithm.h>

#include <iostream>
#include <iomanip>


auto YsonWriter = NYT::NYson::TYsonWriter(&Cout, NYT::NYson::EYsonFormat::Text, NYT::NYson::EYsonType::ListFragment);
auto YsonList = NYT::NYTree::BuildYsonListFragmentFluently(&YsonWriter);

// NOTE(dakovalkov): CreateIOEngine depends on some global variables.
// Use Meyers singletone to ensure that they are initialized before call to CreateIOEngine.
NYT::NIO::IIOEnginePtr GetIOEngine()
{
    static auto engine = CreateIOEngine(NYT::NIO::EIOEngineType::ThreadPool, nullptr);
    return engine;
}

template <class T>
std::optional<ui32> GetProtoExtensionSize(const NYT::NProto::TExtensionSet& extensions)
{
    std::optional<ui32> result;
    i32 tag = NYT::TProtoExtensionTag<T>::Value;
    for (const auto& extension : extensions.extensions()) {
        if (extension.tag() == tag) {
            const auto& data = extension.data();
            result = data.Size();
            break;
        }
    }
    return result;
}

void ProcessChunkMeta(const TFsPath& pathToMeta)
{
    constexpr size_t MetaExtensionLength = 5;
    auto chunkId = pathToMeta.GetName().substr(0, pathToMeta.GetName().Size() - MetaExtensionLength);

    auto chunkReader = New<NYT::NIO::TChunkFileReader>(
        GetIOEngine(),
        NYT::NChunkClient::NullChunkId,
        pathToMeta.Dirname() + "/" + chunkId);

    auto chunkMetaOrError = chunkReader->GetMeta({}).Get();
    if (!chunkMetaOrError.IsOK()) {
        std::cerr << chunkMetaOrError.GetMessage() << "\n";
        return;
    }

    auto chunkMeta = chunkMetaOrError.Value();
    if (chunkMeta->type() != static_cast<int>(NYT::NChunkClient::EChunkType::Table)) {
        return;
    }

    // TODO(dakovalkov): Very old chunks, skip it for now.
    if (!NYT::HasProtoExtension<NYT::NTableClient::NProto::TTableSchemaExt>(chunkMeta->extensions())) {
        std::cerr << "Skipping chunk " << chunkId << " (table schema is missing)\n";
        return;
    }

    auto format = chunkMeta->format();

    auto miscExt = NYT::GetProtoExtension<NYT::NChunkClient::NProto::TMiscExt>(chunkMeta->extensions());
    auto maybeKeyColumnExt = NYT::FindProtoExtension<NYT::NTableClient::NProto::TKeyColumnsExt>(chunkMeta->extensions());
    auto tableSchemaExt = NYT::GetProtoExtension<NYT::NTableClient::NProto::TTableSchemaExt>(chunkMeta->extensions());
    auto tableSchema = NYT::FromProto<NYT::NTableClient::TTableSchema>(tableSchemaExt);
    auto maybeNameTableExt = NYT::FindProtoExtension<NYT::NTableClient::NProto::TNameTableExt>(chunkMeta->extensions());
    auto nameTable = maybeNameTableExt ? NYT::FromProto<NYT::NTableClient::TNameTablePtr>(*maybeNameTableExt) : NYT::NTableClient::TNameTable::FromSchema(tableSchema);
    auto maybeColumnarStatisticsExt = NYT::FindProtoExtension<NYT::NTableClient::NProto::TColumnarStatisticsExt>(chunkMeta->extensions());

    auto getColumnDataWeight = [&] (std::string_view name) -> int64_t {
        auto id = nameTable->FindId(name);
        return *id < maybeColumnarStatisticsExt->column_data_weights_size() ? maybeColumnarStatisticsExt->column_data_weights(*id) : 0;
    };

    auto stringColumnAvgDW100Count = maybeColumnarStatisticsExt
        ? std::optional(CountIf(tableSchemaExt.columns(), [&] (const auto& column) {
            return column.type() == static_cast<int>(NYT::NTableClient::EValueType::String)
                && getColumnDataWeight(column.name()) > 100 * miscExt.row_count();
        }))
        : std::nullopt;

    auto stringColumnAvgDW200Count = maybeColumnarStatisticsExt
        ? std::optional(CountIf(tableSchemaExt.columns(), [&] (const auto& column) {
            return column.type() == static_cast<int>(NYT::NTableClient::EValueType::String)
                && getColumnDataWeight(column.name()) > 200 * miscExt.row_count();
        }))
        : std::nullopt;

    THashMap<int, int> columnTypeCounts;
    THashMap<int, int> simpleLogicalTypeCounts;

    for (const auto& column : tableSchemaExt.columns()) {
        ++columnTypeCounts[column.type()];
        ++simpleLogicalTypeCounts[column.simple_logical_type()];
    }

    YsonList.Item()
        .BeginMap()
            .Item("chunk_id").Value(chunkId)
            .Item("format").Value(format)
            .Item("uncompressed_data_size").Value(
                miscExt.has_uncompressed_data_size() ? std::optional(miscExt.uncompressed_data_size()) : std::nullopt)
            .Item("compressed_data_size").Value(
                miscExt.has_compressed_data_size() ? std::optional(miscExt.compressed_data_size()) : std::nullopt)
            .Item("data_weight").Value(
                miscExt.has_data_weight() ? std::optional(miscExt.data_weight()) : std::nullopt)
            .Item("meta_size").Value(
                miscExt.has_meta_size() ? std::optional(miscExt.meta_size()) : std::nullopt)
            .Item("row_count").Value(
                miscExt.has_row_count() ? std::optional(miscExt.row_count()) : std::nullopt)
            .Item("sorted").Value(
                miscExt.has_sorted() ? std::optional(miscExt.sorted()) : std::nullopt)
            .Item("unique_keys").Value(
                miscExt.has_unique_keys() ? std::optional(miscExt.unique_keys()) : std::nullopt)
            .Item("creation_time").Value(
                miscExt.has_creation_time() ? std::optional(miscExt.creation_time()) : std::nullopt)
            .Item("key_column_count").Value(maybeKeyColumnExt ? std::optional(maybeKeyColumnExt->names_size()) : std::nullopt)
            .Item("table_schema_column_count").Value(tableSchemaExt.columns_size())
            .Item("strict").Value(tableSchemaExt.has_strict() ? std::optional(tableSchemaExt.strict()) : std::nullopt)
            .Item("column_type_counts").Value(columnTypeCounts)
            .Item("simple_logical_type_counts").Value(simpleLogicalTypeCounts)
            .Item("required_column_count").Value(CountIf(tableSchemaExt.columns(), [] (const auto& column) {
                return column.required();
            }))
            .Item("name_table_column_count").Value(
                maybeNameTableExt ? std::optional(maybeNameTableExt->names_size()) : std::nullopt)
            .Item("heavy_column_statistics_ext_size").Value(
                GetProtoExtensionSize<NYT::NTableClient::NProto::THeavyColumnStatisticsExt>(chunkMeta->extensions()))
            .Item("columnar_statistics_ext_size").Value(
                GetProtoExtensionSize<NYT::NTableClient::NProto::TColumnarStatisticsExt>(chunkMeta->extensions()))
            .Item("misc_ext_size").Value(
                GetProtoExtensionSize<NYT::NChunkClient::NProto::TMiscExt>(chunkMeta->extensions()))
            .Item("hunk_chunk_refs_ext_size").Value(
                GetProtoExtensionSize<NYT::NTableClient::NProto::THunkChunkRefsExt>(chunkMeta->extensions()))
            .Item("hunk_chunk_misc_ext_size").Value(
                GetProtoExtensionSize<NYT::NTableClient::NProto::THunkChunkMiscExt>(chunkMeta->extensions()))
            .Item("boundary_keys_ext_size").Value(
                GetProtoExtensionSize<NYT::NTableClient::NProto::TBoundaryKeysExt>(chunkMeta->extensions()))
            .Item("string_column_count").Value(CountIf(tableSchemaExt.columns(), [] (const auto& column) {
                return column.type() == static_cast<int>(NYT::NTableClient::EValueType::String);
            }))
            .Item("string_column_avg_dw100_count").Value(stringColumnAvgDW100Count)
            .Item("string_column_avg_dw200_count").Value(stringColumnAvgDW200Count)
            .Item("table_schema_key_column_count").Value(tableSchema.GetKeyColumnCount())
        .EndMap();
}

void TraverseDisks(const TFsPath& currentPath);
void TraverseSuffixes(const TFsPath& currentPath);
void TraverseChunkMetas(const TFsPath& currentPath);

struct TProgressState
{
    TInstant StartTime;
    TInstant LastPrintProgress;

    size_t TotalDiskCount = 0;
    size_t ProcessedDiskCount = 0;

    size_t CurrentPartCount = 0;
    size_t CurrentlyProcessedPartCount = 0;

    size_t TotalSeenPartCount = 0;
    size_t TotalProcessedPartCount = 0;

    size_t CurrentChunkCount = 0;
    size_t CurrentlyProcessedChunkCount = 0;

    size_t TotalSeenChunkCount = 0;
    size_t TotalProcessedChunkCount = 0;
};

TProgressState ProgressState;

void PrintProgress()
{
    constexpr TDuration PrintProgressThreshold = TDuration::MilliSeconds(500);

    bool finishedDiskProcessing =
        (ProgressState.CurrentlyProcessedChunkCount == ProgressState.CurrentChunkCount &&
        ProgressState.CurrentlyProcessedPartCount + 1 == ProgressState.CurrentPartCount);

    if (finishedDiskProcessing || TInstant::Now() - ProgressState.LastPrintProgress >= PrintProgressThreshold) {

        double estimatedPartCount =
            ProgressState.TotalSeenPartCount *
            static_cast<double>(ProgressState.TotalDiskCount) /
            (ProgressState.ProcessedDiskCount + 1);

        double estimatedChunkCount =
            ProgressState.TotalSeenChunkCount *
            estimatedPartCount /
            (ProgressState.TotalProcessedPartCount + 1);

        double progress = static_cast<double>(ProgressState.TotalProcessedChunkCount) / estimatedChunkCount;

        std::cerr << "progress: " << std::setprecision(1) << std::fixed <<
                progress * 100 << "%    " <<
                "disks: " << ProgressState.ProcessedDiskCount << "/" << ProgressState.TotalDiskCount << "    " <<
                "parts: " << ProgressState.CurrentlyProcessedPartCount << "/" << ProgressState.CurrentPartCount << "    " <<
                "chunks: " << ProgressState.CurrentlyProcessedChunkCount << "/" << ProgressState.CurrentChunkCount << "    " <<
                "et: " << (TInstant::Now() - ProgressState.StartTime).ToString() << "    " <<
                "eta: " << ((TInstant::Now() - ProgressState.StartTime) * (1 - progress) / progress).ToString() << "    ";

        if (finishedDiskProcessing) {
            std::cerr << "\n";
        } else {
            std::cerr << "\r";
        }

        ProgressState.LastPrintProgress = TInstant::Now();
    }
}

void TraverseDisks(const TFsPath& currentPath)
{
    std::cerr << "Started traversing node\n\n";
    ProgressState.StartTime = TInstant::Now();

    TVector<TString> children;
    currentPath.ListNames(children);
    std::vector<TString> disks;
    for (const auto& child : children) {
        if (child.StartsWith("disk")) {
            disks.emplace_back(child);
            ++ProgressState.TotalDiskCount;
        }
    }

    for (const auto& child : disks) {
        TraverseSuffixes(currentPath / child / "hahn-data" / "chunk_store");
        ++ProgressState.ProcessedDiskCount;
    }

    std::cerr << "\navg time/chunk: " << ((TInstant::Now() - ProgressState.StartTime) / std::max<size_t>(ProgressState.TotalProcessedChunkCount, 1)).ToString() << "\n";
}

void TraverseSuffixes(const TFsPath& currentPath)
{
    ProgressState.CurrentPartCount = 0;
    ProgressState.CurrentlyProcessedPartCount = 0;
    TVector<TString> children;
    currentPath.ListNames(children);
    std::vector<TString> suffixes;
    for (const auto& child : children) {
        if (child.length() == 2) {
            suffixes.emplace_back(child);
            ++ProgressState.CurrentPartCount;
        }
    }
    ProgressState.TotalSeenPartCount += ProgressState.CurrentPartCount;

    for (const auto& child : suffixes) {
        TraverseChunkMetas(currentPath / child);
        ++ProgressState.CurrentlyProcessedPartCount;
        ++ProgressState.TotalProcessedPartCount;
    }
}

void TraverseChunkMetas(const TFsPath& currentPath)
{
    ProgressState.CurrentChunkCount = 0;
    ProgressState.CurrentlyProcessedChunkCount = 0;
    TVector<TString> children;
    currentPath.ListNames(children);
    std::vector<TString> chunkMetas;
    for (const auto& child : children) {
        if (child.EndsWith(".meta")) {
            chunkMetas.emplace_back(child);
            ++ProgressState.CurrentChunkCount;
        }
    }
    ProgressState.TotalSeenChunkCount += ProgressState.CurrentChunkCount;

    for (const auto& child : chunkMetas) {
        ProcessChunkMeta(currentPath / child);
        ++ProgressState.CurrentlyProcessedChunkCount;
        ++ProgressState.TotalProcessedChunkCount;
        PrintProgress();
    }
}

int main(int /*argc*/, char** /*argv*/)
{
    TraverseDisks("./");
    return 0;
}
