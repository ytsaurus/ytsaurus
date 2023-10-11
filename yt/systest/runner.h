#pragma once

#include <yt/systest/dataset.h>
#include <yt/systest/table_dataset.h>
#include <yt/systest/test_home.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NTest {

struct TRunnerConfig
{
    TString HomeDirectory;
    int NumBootstrapRecords;
    int Seed;
    int NumOperations;
    bool EnableRenamesDeletes;

    TRunnerConfig();
};

////////////////////////////////////////////////////////////////////////////////

class TRunner
{
public:
    TRunner(TRunnerConfig runnerConfig, IClientPtr client, NApi::IClientPtr rpcClient);
    void Run();

private:
    struct TDatasetInfo
    {
        const IDataset* Dataset;
        const IDataset* ShallowDataset;

        TStoredDataset Stored;
    };

    TRunnerConfig RunnerConfig_;
    IClientPtr Client_;
    NApi::IClientPtr RpcClient_;

    TTestHome TestHome_;
    TStoredDataset BootstrapInfo_;

    std::vector<TDatasetInfo> Infos_;

    std::vector<std::unique_ptr<IDataset>> DatasetPtrs_;
    std::vector<std::unique_ptr<IOperation>> OperationPtrs_;

    void EnableRenamesDeletes();
    void RenameAndDeleteColumn(const TDatasetInfo& info);

    void RunSortAndReduce(const TDatasetInfo& info, const std::vector<TString>& columns, int sumIndex);
};

}  // namespace NYT::NTest
