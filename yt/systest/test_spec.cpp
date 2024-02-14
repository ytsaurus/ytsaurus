
#include <yt/systest/proto/run_spec.pb.h>
#include <yt/systest/proto/table.pb.h>
#include <yt/systest/proto/test_spec.pb.h>

#include <yt/systest/bootstrap_dataset.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/dataset_operation.h>

#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/systest/test_spec.h>

namespace NYT::NTest {

static NProto::TSystestSpec SortSpecTopologically(NProto::TSystestSpec spec)
{
    // Assume the dependency graph is a tree. That might change later.

    std::vector<std::list<int>> children(spec.table_size());

    std::unordered_map<TString, int> nameToIndex;
    for (int i = 0; i < spec.table_size(); ++i) {
        nameToIndex.emplace(std::pair(spec.table(i).name(), i));
    }
    for (int i = 0; i < spec.table_size(); ++i) {
        const auto& table = spec.table(i);
        if (table.has_parent()) {
            auto pos = nameToIndex.find(table.parent());
            if (pos == nameToIndex.end()) {
                THROW_ERROR_EXCEPTION("Broken spec, parent table %v does not exist", table.parent());
            }
            children[pos->second].push_back(i);
        }
    }

    std::vector<int> order;
    order.reserve(spec.table_size());

    for (int i = 0; i < spec.table_size(); ++i) {
        const auto& table = spec.table(i);
        if (table.has_parent()) {
            continue;
        }
        std::queue<int> bfsQueue;
        bfsQueue.push(i);
        while (!bfsQueue.empty()) {
            int current = bfsQueue.front();
            bfsQueue.pop();

            for (int child : children[current]) {
                bfsQueue.push(child);
            }

            order.push_back(current);
        }
    }

    YT_VERIFY(std::ssize(order) == spec.table_size());

    NProto::TSystestSpec result;
    for (int i = 0; i < spec.table_size(); ++i) {
        result.add_table()->Swap(spec.mutable_table(order[i]));
    }

    return result;
}

static int GetColumnIndexByName(const TTable& table, const TString& columnName)
{
    for (int i = 0; i < std::ssize(table.DataColumns); ++i) {
        if (table.DataColumns[i].Name == columnName) {
            return i;
        }
    }
    YT_VERIFY(false);
}

static bool isNumeric(NProto::EColumnType type)
{
    return type == NProto::EColumnType::EInt8 ||
        type == NProto::EColumnType::EInt16 ||
        type == NProto::EColumnType::EInt64;
}

static bool isLowCardinality(NProto::EColumnType type)
{
    return type == NProto::EColumnType::EInt8 ||
        type == NProto::EColumnType::EInt16;
}

static bool TableContainsLowCardinalityColumn(
    const TTable& table)
{
    for (int i = 0; i < std::ssize(table.DataColumns); ++i) {
        if (!isLowCardinality(table.DataColumns[i].Type)) {
            return true;
        }
    }
    return false;
}

static std::pair<std::vector<TString>, std::vector<TString>> GetSortAndReduceColumns(
    std::mt19937_64& randomEngine,
    const TTable& schema)
{
    std::vector<int> narrow;
    std::vector<int> allColumns;
    for (int i = 0; i < std::ssize(schema.DataColumns); i++) {
        if (isLowCardinality(schema.DataColumns[i].Type)) {
            narrow.push_back(i);
        }
        allColumns.push_back(i);
    }

    YT_VERIFY(!narrow.empty());
    std::shuffle(narrow.begin(), narrow.end(), randomEngine);

    std::uniform_int_distribution<int> dist(0, 9);
    int r = dist(randomEngine);
    if (r > 7 && std::ssize(narrow) >= 3) {
        narrow.resize(3);
    } else if (r > 3 && std::ssize(narrow) >= 2) {
        narrow.resize(2);
    } else {
        narrow.resize(1);
    }

    std::vector<TString> keyColumns;
    for (int index : narrow) {
        keyColumns.push_back(schema.DataColumns[index].Name);
    }

    std::shuffle(allColumns.begin(), allColumns.end(), randomEngine);
    int limit = std::uniform_int_distribution<int>(1, std::ssize(allColumns))(randomEngine);

    std::vector<TString> reduceColumns;
    for (int i = 0; i < limit; i++) {
        reduceColumns.push_back(schema.DataColumns[allColumns[i]].Name);
    }

    return std::pair(keyColumns, reduceColumns);
}

static std::vector<int> indexRangeExcept(int start, int limit, std::vector<int> excepted) {
    std::sort(excepted.begin(), excepted.end());
    std::vector<int> result;
    int index = 0;
    for (int i = start; i < limit; i++) {
        if (index < std::ssize(excepted) && i == excepted[index]) {
            ++index;
            continue;
        }
        result.push_back(i);
    }
    return result;
}

static void PopulateBootstrapTable(NProto::TTableSpec* table, const int NumRecords)
{
    table->set_name("bootstrap");

    auto* bootstrap = table->mutable_bootstrap();
    bootstrap->set_num_records(NumRecords);

    auto* estimates = table->mutable_estimates();
    estimates->set_num_records(NumRecords);
    estimates->set_record_bytes(8);
}

static void PopulateSortReduceTables(
    const TTable& base,
    std::mt19937_64& engine,
    TTable* sortTable,
    NProto::TTableSpec* sortProto,
    TTable* reduceTable,
    NProto::TTableSpec* reduceProto)
{
    std::vector<TString> keyColumns;
    std::vector<TString> reduceColumns;
    std::tie(keyColumns, reduceColumns) = GetSortAndReduceColumns(engine, base);

    ApplySortOperation(base, TSortOperation{keyColumns}, sortTable);

    for (const auto& column : keyColumns) {
        sortProto->mutable_sort()->add_sort_by(column);
        reduceProto->mutable_reduce()->add_reduce_by(column);
    }

    std::vector<std::unique_ptr<IReducer>> columnReducers;

    std::vector<int> nonNumeric;
    for (const TString& reduceColumn : reduceColumns) {
        int index = GetColumnIndexByName(*sortTable, reduceColumn);
        if (isNumeric(sortTable->DataColumns[index].Type)) {
            columnReducers.push_back(std::make_unique<TSumReducer>(*sortTable, index,
                    TDataColumn{"S" + std::to_string(std::ssize(columnReducers)), NProto::EColumnType::EInt64, std::nullopt}));
        } else {
            nonNumeric.push_back(index);
        }
    }
    if (!nonNumeric.empty()) {
        columnReducers.push_back(std::make_unique<TSumHashReducer>(*sortTable, nonNumeric,
            TDataColumn{"S" + std::to_string(std::ssize(columnReducers)), NProto::EColumnType::EInt64, std::nullopt}));
    }
    auto reducer = std::make_unique<TConcatenateColumnsReducer>(*sortTable, std::move(columnReducers));
    reducer->ToProto(reduceProto->mutable_reduce()->mutable_operation());

    TReduceOperation reduceOperation{
        std::move(reducer),
        keyColumns
    };

    std::vector<int> indices;
    *reduceTable = CreateTableFromReduceOperation(*sortTable, reduceOperation, &indices);
}

///////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IMultiMapper> CreateAlterRename(const TTable& source)
{
    const int renameIndex = 5;
    const int numIndices = std::ssize(source.DataColumns);
    YT_VERIFY(renameIndex < numIndices);

    std::vector<std::unique_ptr<IRowMapper>> columnOperations;
    columnOperations.push_back(
        std::make_unique<TIdentityRowMapper>(
            source,
            indexRangeExcept(0, 5, {})
        )
    );
    columnOperations.push_back(
        std::make_unique<TRenameColumnRowMapper>(
            source,
            renameIndex,
            "Y" + std::to_string(renameIndex)
        )
    );
    columnOperations.push_back(
        std::make_unique<TIdentityRowMapper>(
            source,
            indexRangeExcept(6, numIndices, {})
        )
    );

    return std::make_unique<TSingleMultiMapper>(
        source,
        std::make_unique<TConcatenateColumnsRowMapper>(
            source, std::move(columnOperations)));

}

std::unique_ptr<IMultiMapper> CreateAlterRenameAndDelete(std::mt19937_64& engine, const TTable& source)
{
    std::vector<int> indices;
    const int numColumns = std::ssize(source.DataColumns);
    indices.reserve(numColumns);
    for (int i = 0; i < numColumns; ++i) {
        indices.push_back(i);
    }
    std::random_shuffle(indices.begin(), indices.end());
    // At least one and at most all columns are renamed.
    int renameLimit = std::uniform_int_distribution<int32_t>(1, numColumns)(engine);
    // At most all remaining columns are deleted.
    int deleteLimit = std::uniform_int_distribution<int32_t>(renameLimit, numColumns)(engine);

    std::sort(indices.begin(), indices.begin() + renameLimit);
    std::sort(indices.begin() + renameLimit, indices.begin() + deleteLimit);

    std::vector<std::unique_ptr<IRowMapper>> columnOperations;

    std::vector<int> identityColumns;

    for (int column = 0; column < numColumns; ++column) {
        if (std::binary_search(indices.begin(), indices.begin() + renameLimit, column)) {
            if (!identityColumns.empty()) {
                columnOperations.push_back(std::make_unique<TIdentityRowMapper>(source, identityColumns));
                identityColumns.clear();
            }
            columnOperations.push_back(std::make_unique<TRenameColumnRowMapper>(source, column, "R" + source.DataColumns[column].Name));
        } else if (std::binary_search(indices.begin() + renameLimit, indices.begin() + deleteLimit, column)) {
            columnOperations.push_back(std::make_unique<TDecorateWithDeletedColumnRowMapper>(source, source.DataColumns[column].Name));
        } else {
            identityColumns.push_back(column);
        }
    }
    if (!identityColumns.empty()) {
        columnOperations.push_back(std::make_unique<TIdentityRowMapper>(source, identityColumns));
        identityColumns.clear();
    }
    return std::make_unique<TSingleMultiMapper>(
        source,
        std::make_unique<TConcatenateColumnsRowMapper>(source, std::move(columnOperations)));
}

NProto::TSystestSpec GenerateShortSystestSpec(const TTestConfig& config)
{
    NProto::TSystestSpec result;
    PopulateBootstrapTable(result.add_table(), config.NumBootstrapRecords);
    auto bootstrapDataset = std::make_unique<TBootstrapDataset>(config.NumBootstrapRecords);

    TTable bootstrap(bootstrapDataset->table_schema());
    std::vector<TTable> base, sort, reduce, alter, alterSort, alterReduce;

    for (int i = 0; i < config.NumPhases; i++) {
        auto* table = result.add_table();
        auto op = GenerateMultipleColumns(
            i > 0 ? base[i - 1] : bootstrap, config.Multiplier, config.Seed + i);

        op->ToProto(table->mutable_map()->mutable_operation());
        table->set_name("base_" + std::to_string(i));
        if (i > 0) {
            if (config.EnableRenames) {
                table->set_parent("alter_" + std::to_string(i - 1));
            } else {
                table->set_parent("base_" + std::to_string(i - 1));
            }
        } else {
            table->set_parent("bootstrap");
        }

        base.push_back(CreateTableFromMapOperation(*op));
    }

    std::mt19937_64 engine(config.Seed);

    if (config.EnableReduce) {
        for (int i = 0; i < config.NumPhases; i++) {
            auto* sortProto = result.add_table();
            auto* reduceProto = result.add_table();

            sortProto->set_name("sort_" + std::to_string(i));
            sortProto->set_parent("base_" + std::to_string(i));

            reduceProto->set_name("reduce_" + std::to_string(i));
            reduceProto->set_parent("sort_" + std::to_string(i));

            TTable sortTable, reduceTable;
            PopulateSortReduceTables(base[i], engine, &sortTable, sortProto, &reduceTable, reduceProto);

            sort.push_back(sortTable);
            reduce.push_back(reduceTable);
        }
    }

    if (config.EnableRenames) {
        for (int i = 0; i < config.NumPhases; i++) {
            auto* alterTable = result.add_table();
            alterTable->set_name("alter_" + std::to_string(i));
            alterTable->set_parent("base_" + std::to_string(i));

            std::unique_ptr<IMultiMapper> op;
            if (config.EnableDeletes) {
                op = CreateAlterRenameAndDelete(engine, base[i]);
            } else {
                op = CreateAlterRename(base[i]);
            }

            op->ToProto(alterTable->mutable_map()->mutable_operation());
            alter.push_back(CreateTableFromMapOperation(*op));

            if (config.EnableReduce && TableContainsLowCardinalityColumn(alter[i])) {
                auto* sortProto = result.add_table();
                auto* reduceProto = result.add_table();

                sortProto->set_name("alter_sort_" + std::to_string(i));
                sortProto->set_parent("alter_" + std::to_string(i));

                reduceProto->set_name("alter_reduce_" + std::to_string(i));
                reduceProto->set_parent("alter_sort_" + std::to_string(i));

                TTable sortTable, reduceTable;
                PopulateSortReduceTables(alter[i], engine, &sortTable, sortProto, &reduceTable, reduceProto);

                alterSort.push_back(sortTable);
                alterReduce.push_back(reduceTable);
            }
        }
    }

    return SortSpecTopologically(std::move(result));
}

NProto::TSystestSpec GenerateSystestSpec(const TTestConfig& config)
{
    NProto::TSystestSpec result;
    PopulateBootstrapTable(result.add_table(), config.NumBootstrapRecords);
    auto bootstrapDataset = std::make_unique<TBootstrapDataset>(config.NumBootstrapRecords);

    TTable bootstrap(bootstrapDataset->table_schema());

    std::vector<std::vector<TTable>> base(config.NumChains);
    std::vector<std::vector<TTable>> alter(config.NumChains);

    for (int chainIndex = 0; chainIndex < config.NumChains; ++chainIndex) {
        for (int phaseIndex = 0; phaseIndex < config.NumPhases; ++phaseIndex) {
            auto op = GenerateMultipleColumns(
                phaseIndex > 0 ? base[chainIndex][phaseIndex - 1] : bootstrap, config.Multiplier,
                config.Seed * 1000000 + chainIndex * 1000 + phaseIndex);

            auto* table = result.add_table();
            op->ToProto(table->mutable_map()->mutable_operation());
            table->set_name("base_" + std::to_string(chainIndex) + "_" + std::to_string(phaseIndex));

            if (phaseIndex > 0) {
                if (config.EnableRenames) {
                    table->set_parent("alter_" + std::to_string(chainIndex) + "_" + std::to_string(phaseIndex - 1));
                } else {
                    table->set_parent("base_" + std::to_string(chainIndex) + "_" + std::to_string(phaseIndex - 1));
                }
            } else {
                table->set_parent("bootstrap");
            }

            base[chainIndex].push_back(CreateTableFromMapOperation(*op));
        }
    }

    return result;
}

}  // namespace NYT::NTest
