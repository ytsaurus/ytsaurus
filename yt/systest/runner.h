#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <yt/systest/dataset.h>
#include <yt/systest/table_dataset.h>
#include <yt/systest/test_home.h>
#include <yt/systest/validator.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NTest {

struct TRunnerConfig
{
    int NumBootstrapRecords;
    int Seed;
    int NumOperations;

    bool EnableReduce;
    bool EnableRenames;
    bool EnableDeletes;

    TRunnerConfig();
    void RegisterOptions(NLastGetopt::TOpts* opts);
};

////////////////////////////////////////////////////////////////////////////////

class TRunner
{
public:
    TRunner(const TString& pool,
            TRunnerConfig runnerConfig,
            IClientPtr client,
            NApi::IClientPtr rpcClient,
            TTestHome& testHome,
            TValidator& validator);

    void Run();

private:
    struct TMappedDataset
    {
        std::unique_ptr<IDataset> Dataset;
        std::unique_ptr<IMultiMapper> Operation;
        TString SourcePath;
        TString TargetPath;
        const TTable* SourceSchema;
    };

    struct TDatasetInfo
    {
        const IDataset* Dataset;
        const IDataset* ShallowDataset;

        TStoredDataset Stored;
    };

    NYT::NLogging::TLogger Logger;
    TString Pool_;
    TRunnerConfig RunnerConfig_;
    IClientPtr Client_;
    NApi::IClientPtr RpcClient_;

    TTestHome& TestHome_;
    TValidator& Validator_;

    TStoredDataset BootstrapInfo_;

    std::vector<TDatasetInfo> Infos_;

    std::vector<std::unique_ptr<IDataset>> DatasetPtrs_;
    std::vector<std::unique_ptr<IOperation>> OperationPtrs_;

    TMappedDataset RenameAndDeleteColumn(const TDatasetInfo& info);
    TMappedDataset RenameColumn(const TDatasetInfo& info);

    void RunSortAndReduce(const TDatasetInfo& info, const std::vector<TString>& columns, const TString& sumColumn);
    void RunSortAndReduce(std::mt19937& randomEngine, const TDatasetInfo& info);

    void VerifyAndKeep(TMappedDataset mapped);

    TString CloneTableViaMap(const TTable& table, const TString& path);
};

}  // namespace NYT::NTest
