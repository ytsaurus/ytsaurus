#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <yt/systest/proto/test_spec.pb.h>

#include <yt/systest/config.h>
#include <yt/systest/dataset.h>
#include <yt/systest/table_dataset.h>
#include <yt/systest/test_home.h>
#include <yt/systest/validator.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NTest {

////////////////////////////////////////////////////////////////////////////////

class TRunner
{
public:
    TRunner(const TString& pool,
            NProto::TSystestSpec testSpec,
            IClientPtr client,
            NApi::IClientPtr rpcClient,
            TTestHome& testHome,
            TValidator& validator);

    void Run();

private:
    struct TDatasetInfo
    {
        const IDataset* Dataset;
        const IDataset* ShallowDataset;

        TStoredDataset Stored;
    };

    NYT::NLogging::TLogger Logger;
    TString Pool_;
    NProto::TSystestSpec TestSpec_;
    IClientPtr Client_;
    NApi::IClientPtr RpcClient_;

    TTestHome& TestHome_;
    TValidator& Validator_;

    TStoredDataset BootstrapInfo_;

    std::vector<TDatasetInfo> Infos_;

    std::vector<std::unique_ptr<IDataset>> DatasetPtrs_;
    std::vector<std::unique_ptr<IOperation>> OperationPtrs_;

    TString CloneTableViaMap(const TTable& table, const TString& path, const TString& targetPath);

    TDatasetInfo PopulateBootstrapDataset(const NProto::TBootstrap& bootstrap, const TString& path);
    TDatasetInfo PopulateMapDataset(const TString& name, const TDatasetInfo& parent, const NProto::TMapRunSpec& runSpec, const TString& path);
    TDatasetInfo PopulateSortDataset(const TString& name, const TDatasetInfo& parent, const NProto::TSortRunSpec& sort, const TString& path);
    TDatasetInfo PopulateReduceDataset(const TString& name, const TDatasetInfo& parent, const NProto::TReduceRunSpec& runSpec, const TString& path);
};

}  // namespace NYT::NTest
