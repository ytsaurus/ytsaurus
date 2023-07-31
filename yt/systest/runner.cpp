
#include <library/cpp/yt/logging/logger.h>

#include <yt/systest/dataset.h>
#include <yt/systest/dataset_operation.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/operation/reduce.h>
#include <yt/systest/run.h>

#include <yt/systest/runner.h>

namespace NYT::NTest {

Runner::Runner(IClientPtr client, int numOperations, int seed, TTestHome* testHome, TStoredDataset bootstrapInfo)
    : Client_(client)
    , NumOperations_(numOperations)
    , Seed_(seed)
    , TestHome_(testHome)
    , BootstrapInfo_(bootstrapInfo)
{
    Infos_.push_back(TDatasetInfo{
        BootstrapInfo_.Dataset,
        BootstrapInfo_.Dataset,
        BootstrapInfo_
    });
}

void Runner::Run()
{
    std::mt19937 RandomEngine(Seed_);

    for (int i = 0; i < NumOperations_; i++) {
        const auto& currentInfo = Infos_.back();

        auto operation = CreateRandomMap(RandomEngine, RandomEngine(), currentInfo.Stored);
        auto dataset = Map(*currentInfo.ShallowDataset, *operation);
        auto path = TestHome_->CreateRandomTablePath();

        RunMap(Client_, *TestHome_, currentInfo.Stored.Path, path, currentInfo.Dataset->table_schema(), *operation);
        auto shallowDataset = std::make_unique<TTableDataset>(dataset->table_schema(), Client_, path);
        auto storedDataset = VerifyTable(Client_, path, *dataset);

        Infos_.push_back(TDatasetInfo{
            dataset.get(),
            shallowDataset.get(),
            storedDataset
        });

        OperationPtrs_.push_back(std::move(operation));
        DatasetPtrs_.push_back(std::move(dataset));
        DatasetPtrs_.push_back(std::move(shallowDataset));
    }

    for (int i = 2; i < NumOperations_; i++) {
        if (Infos_[i].Stored.TotalBytes > (100 << 20)) {
            SortAndReduce(i, Infos_[i]);
        }
    }
}

void Runner::SortAndReduce(int opIndex, const TDatasetInfo& info)
{
    auto sortedPath = TestHome_->CreateRandomTablePath();
    std::vector<TString> columns;
    int index = 0;
    switch (opIndex % 3) {
        case 0:
            columns = {"X2", "X5"};
            index = 4;
            break;
        case 1:
            columns = {"X2", "X0"};
            index = 3;
            break;
        case 2:
            columns = {"X0"};
            index = 3;
            break;
    }

    auto sortedDataset = std::make_unique<TSortDataset>(*info.ShallowDataset, TSortOperation{columns});

    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("Performing sort (InputTable: %v, OutputTable: %v)", info.Stored.Path, sortedPath);

    RunSort(Client_, info.Stored.Path, sortedPath,
        TSortColumns(TVector<TString>(columns.begin(), columns.end())));

    auto reducer = std::make_unique<TSumReducer>(sortedDataset->table_schema(), index, TDataColumn{"S", NProto::EColumnType::EInt64});

    TReduceOperation reduceOperation{
        std::move(reducer),
        columns
    };

    auto reducePath = TestHome_->CreateRandomTablePath();

    YT_LOG_INFO("Performing reduce (InputTable: %v, OutputTable: %v, OpIndex: %v, ReduceIndex: %v)",
        sortedPath, reducePath, opIndex, index);

    RunReduce(Client_, *TestHome_, sortedPath, reducePath, sortedDataset->table_schema(), reduceOperation);

    auto reduceDataset = std::make_unique<TReduceDataset>(*sortedDataset, reduceOperation);

    VerifyTable(Client_, reducePath, *reduceDataset);
}

}  // namespace NYT::NTest
