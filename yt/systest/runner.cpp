
#include <yt/systest/dataset.h>
#include <yt/systest/dataset_operation.h>
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

        auto operation = CreateRandomMap(RandomEngine, currentInfo.Stored);
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
}

}  // namespace NYT::NTest
