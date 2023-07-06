#pragma once

#include <yt/systest/dataset.h>
#include <yt/systest/table_dataset.h>
#include <yt/systest/test_home.h>

namespace NYT::NTest {

class Runner
{
public:
    Runner(IClientPtr client, int numOperations, int seed, TTestHome* testHome, const TStoredDataset bootstrapInfo);

    void Run();

private:
    struct TDatasetInfo
    {
        const IDataset* Dataset;
        const IDataset* ShallowDataset;

        TStoredDataset Stored;
    };

    IClientPtr Client_;
    const int NumOperations_;
    const int Seed_;
    TTestHome* TestHome_;
    const TStoredDataset BootstrapInfo_;

    std::vector<TDatasetInfo> Infos_;

    std::vector<std::unique_ptr<IDataset>> DatasetPtrs_;
    std::vector<std::unique_ptr<IOperation>> OperationPtrs_;
};

}  // namespace NYT::NTest
