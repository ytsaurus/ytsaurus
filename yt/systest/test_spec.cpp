
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

static std::pair<std::vector<TString>, TString> GetSortAndReduceColumns(
    std::mt19937& randomEngine,
    const TTable& schema)
{
    std::uniform_int_distribution<int> dist(0, 5);
    std::vector<int> reduceable;
    std::vector<int> summable;
    for (int i = 0; i < std::ssize(schema.DataColumns); i++) {
        if (isNumeric(schema.DataColumns[i].Type)) {
            summable.push_back(i);
        }
        if (isLowCardinality(schema.DataColumns[i].Type)) {
            reduceable.push_back(i);
        }
    }

    std::shuffle(reduceable.begin(), reduceable.end(), randomEngine);
    std::shuffle(summable.begin(), summable.end(), randomEngine);

    YT_VERIFY(!reduceable.empty());
    YT_VERIFY(!summable.empty());

    std::vector<TString> reduceColumns;
    reduceColumns.push_back(schema.DataColumns[reduceable[0]].Name);
    if (reduceable.size() > 1 && dist(randomEngine) >= 3) {
        reduceColumns.push_back(schema.DataColumns[reduceable[1]].Name);
    }

    return std::make_pair(reduceColumns, schema.DataColumns[summable[0]].Name);
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
    std::mt19937& engine,
    TTable* sortTable,
    NProto::TTableSpec* sortProto,
    TTable* reduceTable,
    NProto::TTableSpec* reduceProto)
{
    std::vector<TString> columns;
    TString reduceColumn;
    std::tie(columns, reduceColumn) = GetSortAndReduceColumns(engine, base);

    ApplySortOperation(base, TSortOperation{columns}, sortTable);

    for (const auto& column : columns) {
        sortProto->mutable_sort()->add_sort_by(column);
        reduceProto->mutable_reduce()->add_reduce_by(column);
    }

    int sumIndex = GetColumnIndexByName(*sortTable, reduceColumn);
    auto reducer = std::make_unique<TSumReducer>(*sortTable, sumIndex,
        TDataColumn{"S", NProto::EColumnType::EInt64, std::nullopt});

    reducer->ToProto(reduceProto->mutable_reduce()->mutable_operation());

    TReduceOperation reduceOperation{
        std::move(reducer),
        columns
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

std::unique_ptr<IMultiMapper> CreateAlterRenameAndDelete(const TTable& source)
{
    const int renameIndex = 5;
    const int deleteIndex = 3;
    const int numIndices = std::ssize(source.DataColumns);

    YT_VERIFY(renameIndex < numIndices);

    std::vector<std::unique_ptr<IRowMapper>> columnOperations;
    columnOperations.push_back(
        std::make_unique<TIdentityRowMapper>(
            source,
            indexRangeExcept(0, 5, {deleteIndex})));

    columnOperations.push_back(
        std::make_unique<TRenameColumnRowMapper>(
            source,
            renameIndex,
            "Y" + std::to_string(renameIndex)));

    columnOperations.push_back(
        std::make_unique<TIdentityRowMapper>(
            source,
            indexRangeExcept(6, numIndices, {})));

    columnOperations.push_back(
        std::make_unique<TDecorateWithDeletedColumnRowMapper>(
            source, source.DataColumns[deleteIndex].Name));

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

    std::mt19937 engine(config.Seed);

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
                op = CreateAlterRenameAndDelete(base[i]);
            } else {
                op = CreateAlterRename(base[i]);
            }

            op->ToProto(alterTable->mutable_map()->mutable_operation());
            alter.push_back(CreateTableFromMapOperation(*op));

            if (config.EnableReduce) {
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

NProto::TSystestSpec GenerateSystestSpec(int seed)
{
    const int NumBootstrapRecords = 100000;
    const int InitialMultiplier = 10;
    const int InitialIterations = 5;

    NProto::TSystestSpec result;

    PopulateBootstrapTable(result.add_table(), NumBootstrapRecords);
    auto bootstrapDataset = std::make_unique<TBootstrapDataset>(NumBootstrapRecords);
    TTable currentTable(bootstrapDataset->table_schema());

    for (int i = 0; i < InitialIterations; i++) {
        auto* table = result.add_table();
        auto op = GenerateMultipleColumns(currentTable, InitialMultiplier, seed + i);
        op->ToProto(table->mutable_map()->mutable_operation());
        table->set_name("base_" + std::to_string(i));
        if (i > 0) {
            table->set_parent("base_" + std::to_string(i - 1));
        } else {
            table->set_parent("bootstrap");
        }

        currentTable = CreateTableFromMapOperation(*op);
    }

    return result;
}

}  // namespace NYT::NTest
