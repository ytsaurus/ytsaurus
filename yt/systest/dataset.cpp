
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/dataset.h>
#include <yt/systest/map_dataset.h>
#include <yt/systest/table.h>
#include <yt/systest/util.h>

#include <stdio.h>

namespace NYT::NTest {

IDatasetIterator::~IDatasetIterator()
{
}

IDataset::~IDataset()
{
}

std::unique_ptr<IDataset> Map(const IDataset& source, const IMultiMapper& operation)
{
    return std::make_unique<TMapDataset>(source, operation);
}

////////////////////////////////////////////////////////////////////////////////

TStoredDataset MaterializeIntoTable(IClientPtr client, const TString& tablePath, const IDataset& dataset)
{
    auto writer = client->CreateTableWriter<TNode>(BuildAttributes(dataset.table_schema()) + tablePath);
    auto iterator = dataset.NewIterator();
    i64 totalSize = 0;
    i64 totalRecords = 0;
    for (; !iterator->Done(); iterator->Next()) {
        ++totalRecords;
        TNode node = TNode::CreateMap();
        for (int i = 0; i < std::ssize(dataset.table_schema().DataColumns); i++) {
            node = node(dataset.table_schema().DataColumns[i].Name, iterator->Values()[i]);
        }
        totalSize += ComputeNodeByteSize(node);
        writer->AddRow(node);
    }
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("Stored table (Path: %v, NumRecords: %v, Bytes: %v)", tablePath, totalRecords, totalSize);
    return TStoredDataset{
        tablePath,
        totalRecords,
        totalSize,
        &dataset
    };
}

TStoredDataset VerifyTable(IClientPtr client,  const TString& tablePath, const IDataset& dataset)
{
    auto reader = client->CreateTableReader<TNode>(tablePath);
    auto iterator = dataset.NewIterator();
    i64 rowIndex = 0;

    const auto& dataColumns = dataset.table_schema().DataColumns;
    auto columnIndex = BuildColumnIndex(dataColumns);

    i64 totalSize = 0;

    for (auto& entry : *reader) {
        if (iterator->Done()) {
            THROW_ERROR_EXCEPTION("Validation failed");
        }
        totalSize += ComputeNodeByteSize(entry.GetRow());
        auto mapRow = entry.GetRow().AsMap();
        auto mapEntry = ArrangeValuesToIndex(columnIndex, mapRow);

        for (int i = 0; i < std::ssize(mapEntry); i++) {
            if (mapEntry[i] != iterator->Values()[i]) {
                THROW_ERROR_EXCEPTION("Validation failed, value mismatch "
                    "(Column: %v, Expected: %v, Actual: %v, RowIndex: %v, ColumnIndex: %v)",
                    dataColumns[i].Name, iterator->Values()[i].ConvertTo<TString>(),
                        mapEntry[i].ConvertTo<TString>(), rowIndex, i);
            }
        }

        ++rowIndex;
        iterator->Next();
    }

    if (!iterator->Done()) {
        THROW_ERROR_EXCEPTION("Validation failed, not enough records read");
    }

    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("Validated table (Path: %v, NumRecords: %v, Bytes: %v)", tablePath, rowIndex, totalSize);

    return {
        tablePath,
        rowIndex,
        totalSize,
        &dataset
    };
}

}  // namespace NYT::NTest
