
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

TStoredDataset MaterializeIgnoringStableNames(IClientPtr client, const TString& tablePath, const IDataset& dataset)
{
    auto writer = client->CreateTableWriter<TNode>(
        BuildAttributes(DropStableNames(dataset.table_schema())) + tablePath);
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
    };
}

}  // namespace NYT::NTest
