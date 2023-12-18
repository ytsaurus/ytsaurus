#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/library/erasure/public.h>

#include <stdlib.h>

#include <iostream>
#include <iomanip>

using namespace NYT;
using namespace NYT::NIO;
using namespace NYT::NChunkClient;
using namespace NYT::NChunkClient::NProto;
using namespace NYT::NTableClient::NProto;
using namespace NYT::NTableClient;
using namespace NYT::NErasure;

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: print_chunk_meta <chunk_meta_file>" << std::endl;
        return 1;
    }

    try {
        auto fileName = TString(argv[1]);

        auto reader = New<TChunkFileReader>(
            CreateIOEngine(EIOEngineType::ThreadPool, NYTree::INodePtr()),
            NullChunkId,
            fileName);

        auto chunkMeta = reader->GetMeta(/* chunkReadOptions */ {})
            .Get()
            .ValueOrThrow();

        std::cout << "Type: " << ToString(FromProto<EChunkType>(chunkMeta->type())) << std::endl;
        std::cout << "Format: " << ToString(FromProto<EChunkFormat>(chunkMeta->format())) << std::endl;

        auto columnMetaExt = FindProtoExtension<TColumnMetaExt>(chunkMeta->extensions());
        if (!columnMetaExt) {
            THROW_ERROR_EXCEPTION("Chunk does not contain columnar meta");
        }

        auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta->extensions());
        auto schema = FromProto<TTableSchema>(schemaExt);

        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
        auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta->extensions());

        std::cout << "Total uncompressed size: " << miscExt.uncompressed_data_size() << std::endl;
        std::cout << "Total compressed size: " << miscExt.compressed_data_size() << std::endl;
        std::cout << std::endl;


        bool isErasure = false;
        if (FromProto<ECodec>(miscExt.erasure_codec()) != ECodec::None) {
            std::cout << "Analysing erasure chunk, compressed statistics unavailable" << std::endl;
            std::cout << std::endl;
            isErasure = true;
        }

        THashMap<TString, std::optional<TString>> columnGroupMapping;
        THashMap<TString, THashSet<int>> groupBlockIds;

        struct TColumnInfo
        {
            TString Name;
            i64 UncompressedSize = 0;
            double UncompressedRatio = 0;
            i64 CompressedSize = 0;
            double CompressedRatio = 0;
            bool HasGroup = false;
        };

        auto gatherSegmentsInfo = [&] (int columnIndex, THashSet<int>* blockIndexes, TColumnInfo* info) {
            const auto& columnMeta = columnMetaExt->columns()[columnIndex];
            i64 UncompressedSize = 0;
            for (auto segment : columnMeta.segments()) {
                UncompressedSize += segment.size();
                blockIndexes->insert(segment.block_index());
            }

            info->UncompressedSize = UncompressedSize;
            info->UncompressedRatio = 100 * double(UncompressedSize) / miscExt.uncompressed_data_size();
        };

        auto getCompressedSize = [&] (const THashSet<int>& blockIndexes) -> i64 {
            i64 result = 0;
            for (auto index : blockIndexes) {
                result += blocksExt.blocks()[index].size();
            }
            return result;
        };

        std::cout <<  "Column statistics: " << std::endl;

        std::vector<TColumnInfo> columnInfos;
        for (int columnIndex = 0; columnIndex < std::ssize(schema.Columns()); ++columnIndex) {
            const auto& column = schema.Columns()[columnIndex];
            columnInfos.emplace_back();
            auto& columnInfo = columnInfos.back();
            columnInfo.Name = column.Name();

            THashSet<int> blockIndexes;

            gatherSegmentsInfo(columnIndex, &blockIndexes, &columnInfo);

            if (!isErasure) {
                if (!column.Group()) {
                    auto compressedSize = getCompressedSize(blockIndexes);
                    columnInfo.CompressedSize = compressedSize;
                    columnInfo.CompressedRatio = 100 * double(compressedSize) / miscExt.compressed_data_size();
                } else {
                    columnInfo.HasGroup = true;
                    groupBlockIds[*column.Group()].insert(blockIndexes.begin(), blockIndexes.end());
                }
            }

        }

        if (!schema.GetStrict()) {
            columnInfos.emplace_back();
            auto& columnInfo = columnInfos.back();
            columnInfo.Name = "== Other ==";

            THashSet<int> blockIndexes;
            int columnIndex = schema.Columns().size();

            gatherSegmentsInfo(columnIndex, &blockIndexes, &columnInfo);

            if (!isErasure) {
                auto compressedSize = getCompressedSize(blockIndexes);
                columnInfo.CompressedSize = compressedSize;
                columnInfo.CompressedRatio = 100 * double(compressedSize) / miscExt.compressed_data_size();
            }
        }

        if (isErasure) {
            std::sort(columnInfos.begin(), columnInfos.end(), [](const TColumnInfo& lhs, const TColumnInfo& rhs) {
                return lhs.UncompressedRatio < rhs.UncompressedRatio;
            });
        } else {
            std::sort(columnInfos.begin(), columnInfos.end(), [](const TColumnInfo& lhs, const TColumnInfo& rhs) {
                return lhs.CompressedRatio < rhs.CompressedRatio;
            });
        }

        for (const auto& info : columnInfos) {
            std::cout
                << std::left << std::setw(70) << info.Name.c_str()
                << std::right << std::setw(20) << info.UncompressedSize << " (" << std::setw(10) << std::setprecision(4) << info.UncompressedRatio  << "%)";

            if (!isErasure && !info.HasGroup) {
                std::cout << std::right << std::setw(20) << info.CompressedSize << " (" << std::setw(10) << std::setprecision(4) << info.CompressedRatio  << "%)";
            }

            std::cout << std::endl;
        }

        std::cout << std::endl;

        if (!groupBlockIds.empty() && !isErasure) {
            std::cout << "Group compressed statistics:" << std::endl;

            for (const auto& pair : groupBlockIds) {
                std::cout << std::setw(70);
                std::cout << std::left << pair.first;

                auto compressedSize = getCompressedSize(pair.second);
                std::cout << std::setw(20) << std::setprecision(4) << compressedSize << " (" << double(compressedSize) / miscExt.compressed_data_size()  << "%)";
            }

            std::cout << std::endl;
        }
    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << std::endl;
    }
}
