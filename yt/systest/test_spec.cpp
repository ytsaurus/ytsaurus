
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

static bool TableContainsLowCardinalityColumn(const TTable& table)
{
    for (int i = 0; i < std::ssize(table.DataColumns); ++i) {
        if (isLowCardinality(table.DataColumns[i].Type)) {
            return true;
        }
    }
    return false;
}

static bool TableContainsInt8Column(const TTable& table)
{
    for (int i = 0; i < std::ssize(table.DataColumns); ++i) {
        if (table.DataColumns[i].Type == NProto::EColumnType::EInt8) {
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
    int depth,
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

    auto nextColumnName = [&]() {
        return std::string(1, 'S' + depth) + std::to_string(std::ssize(columnReducers));
    };

    std::vector<int> nonNumeric;
    for (const TString& reduceColumn : reduceColumns) {
        int index = GetColumnIndexByName(*sortTable, reduceColumn);
        if (isNumeric(sortTable->DataColumns[index].Type)) {
            columnReducers.push_back(std::make_unique<TSumReducer>(*sortTable, index,
                    TDataColumn{nextColumnName(), NProto::EColumnType::EInt64, std::nullopt}));
        } else {
            nonNumeric.push_back(index);
        }
    }
    if (!nonNumeric.empty()) {
        columnReducers.push_back(std::make_unique<TSumHashReducer>(*sortTable, nonNumeric,
            TDataColumn{nextColumnName(), NProto::EColumnType::EInt8, std::nullopt}));
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

static std::unique_ptr<IMultiMapper> CreateAlterRename(const TTable& source)
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

static std::unique_ptr<IMultiMapper> CreateAlterRenameAndDelete(std::mt19937_64& engine, const TTable& source)
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

static std::unique_ptr<IMultiMapper> CreateFilter(std::mt19937_64& engine, const TTable& source)
{
    std::vector<int> indices;
    for (int i = 0; i < std::ssize(source.DataColumns); ++i) {
        if (source.DataColumns[i].Type == NProto::EColumnType::EInt8) {
            indices.push_back(i);
        }
    }
    YT_VERIFY(!indices.empty());
    int column = indices[std::uniform_int_distribution<int>(0, std::ssize(indices) - 1)(engine)];
    auto type = source.DataColumns[column].Type;

    switch (type) {
        case NProto::EColumnType::EInt8: {
            return std::make_unique<TFilterMultiMapper>(source, column,
                std::uniform_int_distribution<int8_t>(0, 127)(engine));
        }
        default: {
            YT_VERIFY(false);  // Must be a low cardinality column, one of these two types.
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

class TSystestSpecGenerator
{
public:
    explicit TSystestSpecGenerator(const TTestConfig& config)
        : Config_(config)
    {
    }

    NProto::TSystestSpec Generate();

private:
    const TTestConfig Config_;
    NProto::TSystestSpec Result_;

    std::vector<TTable> Tables_;
    std::vector<TString> TableNames_;

    int AddMap(const TString& name, int seed, int parent, int multiplier);
    int AddAlter(const TString& name, int seed, int parent);
    int AddSortReduce(const TString& sortName, const TString& reduceName, int depth, int seed, int parent);
    int AddFilter(const TString& name, int seed, int parent);

    void CreateBasicChain(const TString& chainName, int seed, int startIndex, int multiplier, int chainLength);
    int CreateFullChain(const TString& chainName, int seed, int startIndex, int multiplier, int startNameIndex, int chainLength);

};

int TSystestSpecGenerator::AddMap(const TString& name, int seed, int parent, int multiplier)
{
    auto op = GenerateMultipleColumns(Tables_.at(parent), multiplier, seed);

    auto* table = Result_.add_table();
    op->ToProto(table->mutable_map()->mutable_operation());
    table->set_name(name);
    table->set_parent(TableNames_.at(parent));

    Tables_.push_back(CreateTableFromMapOperation(*op));
    TableNames_.push_back(table->name());

    return std::ssize(Tables_) - 1;
}

int TSystestSpecGenerator::AddAlter(const TString& name, int seed, int parent)
{
    auto* alterTable = Result_.add_table();
    alterTable->set_name(name);
    alterTable->set_parent(TableNames_.at(parent));

    std::unique_ptr<IMultiMapper> op;

    std::mt19937_64 engine(seed);
    if (Config_.EnableDeletes) {
        op = CreateAlterRenameAndDelete(engine, Tables_.at(parent));
    } else {
        op = CreateAlterRename(Tables_.at(parent));
    }

    op->ToProto(alterTable->mutable_map()->mutable_operation());

    Tables_.push_back(CreateTableFromMapOperation(*op));
    TableNames_.push_back(alterTable->name());
    return std::ssize(Tables_) - 1;
}

int TSystestSpecGenerator::AddFilter(const TString& name, int seed, int parent)
{
    auto* filterTable = Result_.add_table();
    filterTable->set_name(name);
    filterTable->set_parent(TableNames_.at(parent));

    std::mt19937_64 engine(seed);
    std::unique_ptr<IMultiMapper> op = CreateFilter(engine, Tables_.at(parent));
    op->ToProto(filterTable->mutable_map()->mutable_operation());

    Tables_.push_back(CreateTableFromMapOperation(*op));
    TableNames_.push_back(filterTable->name());
    return std::ssize(Tables_) - 1;
}

int TSystestSpecGenerator::AddSortReduce(
    const TString& sortName,
    const TString& reduceName,
    int depth,
    int seed,
    int parent)
{
    auto* sortProto = Result_.add_table();
    auto* reduceProto = Result_.add_table();

    sortProto->set_name(sortName);
    sortProto->set_parent(TableNames_.at(parent));

    reduceProto->set_name(reduceName);
    reduceProto->set_parent(sortName);

    std::mt19937_64 engine(seed);
    TTable sortTable, reduceTable;
    PopulateSortReduceTables(Tables_[parent], depth, engine, &sortTable, sortProto, &reduceTable, reduceProto);

    Tables_.push_back(sortTable);
    TableNames_.push_back(sortName);

    Tables_.push_back(reduceTable);
    TableNames_.push_back(reduceName);
    return std::ssize(Tables_) - 1;
}

void TSystestSpecGenerator::CreateBasicChain(const TString& chainName, int seed, int startIndex, int multiplier, int chainLength)
{
    int baseIndex = -1;
    int alterIndex = -1;
    for (int phaseIndex = 0; phaseIndex < chainLength; ++phaseIndex) {
        int parentIndex;
        if (phaseIndex == 0) {
            parentIndex = startIndex;
        } else {
            parentIndex = Config_.EnableRenames ? alterIndex : baseIndex;
        }
        baseIndex = AddMap(
            chainName + "_base_" + std::to_string(phaseIndex),
            seed * 1000 + phaseIndex * 10,
            parentIndex,
            multiplier);

        if (Config_.EnableReduce) {
            AddSortReduce(
                chainName + "_sort_" + std::to_string(phaseIndex),
                chainName + "_reduce_" + std::to_string(phaseIndex),
                0,  /*depth*/
                seed * 1000 + phaseIndex * 10 + 1,
                baseIndex);
        }

        if (Config_.EnableRenames) {
            alterIndex = AddAlter(
                chainName + "_alter_" + std::to_string(phaseIndex),
                seed * 1000 + phaseIndex * 10 + 2,
                baseIndex);

            if (Config_.EnableReduce && TableContainsLowCardinalityColumn(Tables_[alterIndex])) {
                AddSortReduce(
                    chainName + "_alter_sort_" + std::to_string(phaseIndex),
                    chainName + "_alter_reduce_" + std::to_string(phaseIndex),
                    0,  /*depth*/
                    seed * 1000 + phaseIndex * 10 + 3,
                    alterIndex);
            }
        }
    }
}

int TSystestSpecGenerator::CreateFullChain(const TString& chainName, int seed, int startIndex, int multiplier, int startNameIndex, int chainLength)
{
    YT_VERIFY(Config_.EnableRenames && Config_.EnableReduce);

    auto phaseName = [&](const char* opName, int phaseIndex) {
        return chainName + std::to_string(phaseIndex + startNameIndex) + "_" + opName;
    };

    int alterIndex = -1;
    for (int phaseIndex = 0; phaseIndex < chainLength; ++phaseIndex) {
        int parentIndex = phaseIndex == 0 ? startIndex : alterIndex;
        int baseIndex = AddMap(
            phaseName("base", phaseIndex),
            seed * 1000 + phaseIndex * 10,
            parentIndex,
            multiplier);

        AddSortReduce(
            phaseName("sort", phaseIndex),
            phaseName("reduce", phaseIndex),
            0,  /*depth*/
            seed * 1000 + phaseIndex * 10 + 1,
            baseIndex);

        alterIndex = AddAlter(
            phaseName("alter", phaseIndex),
            seed * 1000 + phaseIndex * 10 + 2,
            baseIndex);

        if (TableContainsLowCardinalityColumn(Tables_[alterIndex])) {
            int alterReduceIndex = AddSortReduce(
                phaseName("alter_sort", phaseIndex),
                phaseName("alter_reduce", phaseIndex),
                0,  /*depth*/
                seed * 1000 + phaseIndex * 10 + 3,
                alterIndex);

            if (TableContainsInt8Column(Tables_[alterIndex])) {
                AddFilter(
                    phaseName("alter_filter", phaseIndex),
                    seed * 1000 + phaseIndex * 10 + 4,
                    alterIndex);
            }

            if (TableContainsInt8Column(Tables_[alterReduceIndex])) {
                int alterReduceFilterIndex = AddFilter(
                    phaseName("alter_reduce_filter", phaseIndex),
                    seed * 1000 + phaseIndex * 10 + 5,
                    alterReduceIndex);

                AddSortReduce(
                    phaseName("alter_reduce_sort", phaseIndex),
                    phaseName("alter_reduce_reduce", phaseIndex),
                    1,  /*depth*/
                    seed * 1000 + phaseIndex * 10 + 3,
                    alterReduceIndex);

                if (TableContainsLowCardinalityColumn(Tables_[alterReduceFilterIndex])) {
                    AddSortReduce(
                        phaseName("alter_reduce_filter_sort", phaseIndex),
                        phaseName("alter_reduce_filter_reduce", phaseIndex),
                        1,  /*depth*/
                        seed * 1000 + phaseIndex * 10 + 3,
                        alterReduceFilterIndex);
                }
            }
        }
    }
    return alterIndex;
}

NProto::TSystestSpec TSystestSpecGenerator::Generate()
{
    PopulateBootstrapTable(Result_.add_table(), Config_.NumBootstrapRecords);
    auto bootstrapDataset = std::make_unique<TBootstrapDataset>(Config_.NumBootstrapRecords);
    TTable bootstrap(bootstrapDataset->table_schema());

    Tables_.push_back(bootstrap);
    TableNames_.push_back("bootstrap");

    if (Config_.Preset == "short") {
        CreateBasicChain("s", Config_.Seed, 0, Config_.Multiplier, Config_.N);
        return SortSpecTopologically(Result_);
    }

    for (int i = 0; i < Config_.N; ++i) {
        int e1 = CreateFullChain("s" + std::to_string(i) + "a", Config_.Seed * 100 + 10 * i, 0, Config_.Multiplier, 0, Config_.LengthA);
        int e2 = CreateFullChain("s" + std::to_string(i) + "b", Config_.Seed * 100 + 10 * i + 1, e1, Config_.Multiplier, Config_.LengthA, Config_.LengthB);

        CreateFullChain("s" + std::to_string(i) + "c", Config_.Seed * 100 + 10 * i + 2, e1, 1, Config_.LengthA, Config_.LengthC);
        CreateFullChain("s" + std::to_string(i) + "d", Config_.Seed * 100 + 10 * i + 3, e2, 1, Config_.LengthA + Config_.LengthB, Config_.LengthD);
    }

    return SortSpecTopologically(Result_);
}

NProto::TSystestSpec GenerateSystestSpec(const TTestConfig& config)
{
    TSystestSpecGenerator generator(config);
    return generator.Generate();
}

}  // namespace NYT::NTest
