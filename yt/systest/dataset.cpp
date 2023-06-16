
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/dataset.h>
#include <yt/systest/map_dataset.h>

namespace NYT::NTest {

IDatasetIterator::~IDatasetIterator()
{
}

IDataset::~IDataset()
{
}

std::unique_ptr<IDataset> Map(const IDataset& source, const IMultiMapper& operation)
{
    return std::make_unique<MapDataset>(source, operation);
}

////////////////////////////////////////////////////////////////////////////////

void MaterializeIntoTable(IClientPtr client, const TString& tablePath, const IDataset& dataset)
{
    auto writer = client->CreateTableWriter<TNode>(tablePath);
    auto iterator = dataset.NewIterator();
    for (; !iterator->Done(); iterator->Next()) {
        TNode node = TNode::CreateMap();
        for (int i = 0; i < std::ssize(dataset.table_schema().DataColumns); i++) {
            node = node(dataset.table_schema().DataColumns[i].Name, iterator->Values()[i]);
            writer->AddRow(node);
        }
    }
}

void VerifyTable(IClientPtr client,  const TString& tablePath, const IDataset& dataset)
{
    auto reader = client->CreateTableReader<TNode>(tablePath);
    auto iterator = dataset.NewIterator();
    int rowIndex = 0;

    std::unordered_map<TString, int> columnIndex;

    const std::vector<TDataColumn>& dataColumns = dataset.table_schema().DataColumns;

    for (int i = 0; i < std::ssize(dataColumns); i++) {
        const auto& columnName = dataColumns[i].Name;
        columnIndex[columnName] = i;
    }

    for (auto& entry : *reader) {
        if (iterator->Done()) {
            THROW_ERROR_EXCEPTION("Validation failed");
        }
        auto mapRow = entry.GetRow().AsMap();
        int i = 0;

        std::vector<std::pair<int, TNode>> mapEntry;
        for (const auto& entry : mapRow) {
            auto iteratorColumnPos = columnIndex.find(entry.first);
            if (iteratorColumnPos == columnIndex.end()) {
                THROW_ERROR_EXCEPTION("Validation failed, unexpected column (ColumnName: %v)", entry.first);
            }
            mapEntry.push_back(std::make_pair(iteratorColumnPos->second, entry.second));
        }

        if (std::ssize(mapEntry) != std::ssize(dataColumns)) {
            THROW_ERROR_EXCEPTION("Validation failed, wrong number of columns (Expected: %v)",
                std::ssize(dataColumns));
        }

        std::sort(mapEntry.begin(), mapEntry.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.first < rhs.first;
            });

        for (const auto& entry : mapEntry) {
            if (entry.second != iterator->Values()[i]) {
                THROW_ERROR_EXCEPTION("Validation failed, value mismatch (Column: %v, Expected: %v, Actual: %v, RowIndex: %v)",
                    dataColumns[i].Name, iterator->Values()[i].ConvertTo<TString>(),
                        entry.second.ConvertTo<TString>(), rowIndex);
            }
            ++i;
        }

        ++rowIndex;
        iterator->Next();
    }

    if (!iterator->Done()) {
        THROW_ERROR_EXCEPTION("Validation failed, not enough records read");
    }

    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("Validated table (Path: %v, NumRecords: %v)", tablePath, rowIndex);
}

}  // namespace NYT::NTest
