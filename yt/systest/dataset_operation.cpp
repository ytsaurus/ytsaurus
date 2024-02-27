
#include <library/cpp/yt/logging/logger.h>
#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/systest/dataset_operation.h>

namespace NYT::NTest {

std::unique_ptr<IMultiMapper> GenerateRandomColumn(const TTable& table, int seed)
{
    std::vector<std::unique_ptr<IRowMapper>> singleOperations;
    singleOperations.push_back(std::make_unique<TSetSeedRowMapper>(table, 0, seed));

    return std::make_unique<TCombineMultiMapper>(
        table,
        std::move(singleOperations),
        std::make_unique<TRepeatMultiMapper>(
            table,
            10,
            std::make_unique<TGenerateRandomRowMapper>(
                table,
                TDataColumn{"X", NProto::EColumnType::EInt64, std::nullopt}
            )
        )
    );
}

std::unique_ptr<IMultiMapper> GenerateMultipleColumns(const TTable& table, int RowMultipler, int seed)
{
    std::vector<std::unique_ptr<IRowMapper>> randomColumns;
    randomColumns.push_back(std::make_unique<TGenerateRandomRowMapper>(
                table,
                TDataColumn{"X0", NProto::EColumnType::EInt8, std::nullopt}));

    for (int i = 1; i < 10; i++) {
        NProto::EColumnType type;
        switch (i % 3) {
            case 0:
                type = NProto::EColumnType::EInt64;
                break;
            case 1:
                type = NProto::EColumnType::ELatinString100;
                break;
            case 2:
                type = NProto::EColumnType::EInt16;
                break;
        }

        TString columnName = "X" + std::to_string(i);

        randomColumns.push_back(std::make_unique<TGenerateRandomRowMapper>(
                table,
                TDataColumn{columnName, type, std::nullopt}));
    }
    randomColumns.push_back(std::make_unique<TGenerateRandomRowMapper>(
            table,
            TDataColumn{"X10", NProto::EColumnType::EInt8, std::nullopt}));
    randomColumns.push_back(std::make_unique<TGenerateRandomRowMapper>(
            table,
            TDataColumn{"X11", NProto::EColumnType::EInt8, std::nullopt}));

    std::vector<std::unique_ptr<IRowMapper>> singleOperations;
    singleOperations.push_back(std::make_unique<TSetSeedRowMapper>(table, 0, seed));

    return std::make_unique<TCombineMultiMapper>(
        table,
        std::move(singleOperations),
        std::make_unique<TRepeatMultiMapper>(
            table,
            RowMultipler,
            std::make_unique<TConcatenateColumnsRowMapper>(table, std::move(randomColumns))
        )
    );
}

std::unique_ptr<IMultiMapper> FilterByInt8(const TTable& table, int8_t value)
{
    return std::make_unique<TFilterMultiMapper>(table, 0, value);
}

std::unique_ptr<IMultiMapper> CreateRandomMap(
    std::mt19937_64& randomEngine, int seed, const TTable& table, const TStoredDataset& info)
{
    NYT::NLogging::TLogger Logger("test");
    if (info.TotalBytes < 20000) {
        YT_LOG_INFO("Generate Random Column (InputBytes: %v, InputRecords: %v)",
            info.TotalBytes, info.TotalRecords);
        return GenerateRandomColumn(table, seed);
    } else if (info.TotalBytes > (100 << 20) &&
            table.DataColumns[0].Type == NProto::EColumnType::EInt8) {
        std::uniform_int_distribution<int16_t> dist(0, 127);
        int8_t value = dist(randomEngine);
        YT_LOG_INFO("Filter by uint8 column (InputBytes: %v, InputRecords: %v, Value: %v)",
            info.TotalBytes, info.TotalRecords, value);
        return FilterByInt8(table, value);
    } else {
        const int RowMultipler = 4;
        YT_LOG_INFO("Generate Multiple Columns (InputBytes: %v, InputRecords: %v, RowMultipler: %v)",
            info.TotalBytes, info.TotalRecords, RowMultipler);
        return GenerateMultipleColumns(table, RowMultipler, seed);
    }
}

}  // namespace NYT::NTest
