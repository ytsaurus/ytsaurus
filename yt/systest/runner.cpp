
#include <library/cpp/yt/logging/logger.h>

#include <yt/systest/bootstrap_dataset.h>
#include <yt/systest/dataset.h>
#include <yt/systest/dataset_operation.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/run.h>

#include <yt/systest/operation/map.h>
#include <yt/systest/operation/multi_map.h>
#include <yt/systest/operation/reduce.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/systest/runner.h>

#include <util/string/join.h>

#include <unistd.h>

namespace NYT::NTest {

TRunner::TRunner(
    const TString& pool,
    NProto::TSystestSpec testSpec,
    IClientPtr client,
    NApi::IClientPtr rpcClient,
    TTestHome& testHome,
    TValidator& validator)
    : Logger("test")
    , Pool_(pool)
    , TestSpec_(testSpec)
    , Client_(client)
    , RpcClient_(rpcClient)
    , TestHome_(testHome)
    , Validator_(validator)
{
}

TRunner::TDatasetInfo TRunner::PopulateBootstrapDataset(const NProto::TBootstrap& bootstrap, const TString& path)
{
    std::unique_ptr<IDataset> bootstrapDataset = std::make_unique<TBootstrapDataset>(bootstrap.num_records());
    YT_LOG_INFO("Will write bootstrap table (Path: %v)", path);

    auto bootstrapInfo = MaterializeIgnoringStableNames(Client_, path, *bootstrapDataset);

    TDatasetInfo result{
        bootstrapDataset.get(),
        bootstrapDataset.get(),
        bootstrapInfo
    };

    DatasetPtrs_.push_back(std::move(bootstrapDataset));

    return result;
}

TRunner::TDatasetInfo TRunner::PopulateMapDataset(
    const TString& name,
    const TDatasetInfo& parent,
    const NProto::TMapRunSpec& runSpec,
    const TString& path)
{
    const auto& input = parent.Dataset->table_schema();
    auto mapOperation = CreateFromProto(input, runSpec.operation());

    auto dataset = Map(*parent.ShallowDataset, *mapOperation);

    YT_LOG_INFO("Performing Map (InputTable: %v, OutputTable: %v)", parent.Stored.Path, path);

    if (mapOperation->Alterable()) {
        CloneTableViaMap(parent.Dataset->table_schema(), parent.Stored.Path, path);
        AlterTable(RpcClient_, path, dataset->table_schema());
    } else {
        RunMap(
            Client_,
            Pool_,
            TestHome_,
            parent.Stored.Path,
            path,
            parent.Dataset->table_schema(),
            dataset->table_schema(),
            *mapOperation);
    }

    auto stored = Validator_.VerifyMap(
        name,
        parent.Stored.Path,
        path,
        input,
        *mapOperation);

    auto shallowDataset = std::make_unique<TTableDataset>(
        dataset->table_schema(), Client_, path);

    TDatasetInfo result{
        dataset.get(),
        shallowDataset.get(),
        stored
    };

    DatasetPtrs_.push_back(std::move(shallowDataset));
    DatasetPtrs_.push_back(std::move(dataset));
    OperationPtrs_.push_back(std::move(mapOperation));

    return result;
}

TRunner::TDatasetInfo TRunner::PopulateReduceDataset(
    const TString& name,
    const TDatasetInfo& parent,
    const NProto::TReduceRunSpec& runSpec,
    const TString& path)
{
    const auto& input = parent.Dataset->table_schema();

    std::vector<TString> columns(runSpec.reduce_by().begin(), runSpec.reduce_by().end());

    TReduceOperation reduceOperation{
        CreateFromProto(input, runSpec.operation()),
        columns
    };

    auto reduceDataset = std::make_unique<TReduceDataset>(*parent.ShallowDataset, reduceOperation);

    YT_LOG_INFO("Performing reduce (InputTable: %v, Columns: %v, OutputTable: %v)",
        parent.Stored.Path, JoinSeq(",", columns), path);

    RunReduce(Client_,
              Pool_,
              TestHome_,
              parent.Stored.Path,
              path,
              input,
              reduceDataset->table_schema(),
              reduceOperation);

    auto stored = Validator_.VerifyReduce(
        name,
        parent.Stored.Path,
        path,
        input,
        reduceOperation);

    auto shallowDataset = std::make_unique<TTableDataset>(
        reduceDataset->table_schema(), Client_, path);

    TDatasetInfo result{
        reduceDataset.get(),
        shallowDataset.get(),
        stored
    };

    DatasetPtrs_.push_back(std::move(shallowDataset));
    DatasetPtrs_.push_back(std::move(reduceDataset));
    OperationPtrs_.push_back(std::move(reduceOperation.Reducer));

    return result;
}

TRunner::TDatasetInfo TRunner::PopulateSortDataset(
    const TString& name,
    const TDatasetInfo &parent,
    const NProto::TSortRunSpec& sort,
    const TString& path)
{
    const auto& input = parent.Dataset->table_schema();

    std::vector<TString> columns;
    for (const auto& column : sort.sort_by()) {
        columns.push_back(column);
    }
    TSortOperation sortOperation{columns};

    TString sortColumnsString = JoinSeq(",", columns);
    YT_LOG_INFO("Performing sort (InputTable: %v, Columns: %v, OutputTable: %v)", parent.Stored.Path, sortColumnsString, path);

    auto sortDataset = std::make_unique<TSortDataset>(*parent.ShallowDataset, sortOperation);

    RunSort(Client_, Pool_, parent.Stored.Path, path,
        TSortColumns(TVector<TString>(columns.begin(), columns.end())));

    auto stored = Validator_.VerifySort(
        name,
        parent.Stored.Path,
        path,
        input,
        sortOperation);

    auto shallowDataset = std::make_unique<TTableDataset>(
        sortDataset->table_schema(), Client_, path);

    TDatasetInfo result{
        sortDataset.get(),
        shallowDataset.get(),
        stored
    };

    DatasetPtrs_.push_back(std::move(shallowDataset));
    DatasetPtrs_.push_back(std::move(sortDataset));

    return result;
}

void TRunner::Run()
{
    std::unordered_map<TString, int> nameIndex;
    for (const auto& table : TestSpec_.table()) {
        nameIndex[table.name()] = std::ssize(Infos_);
        const TDatasetInfo* parent = nullptr;
        if (table.has_parent()) {
            auto pos = nameIndex.find(table.parent());
            if (pos == nameIndex.end()) {
                THROW_ERROR_EXCEPTION("Unknown parent table %v (current table %v)", table.parent(), table.name());
            }
            parent = &Infos_[pos->second];
        }
        TString name{table.name()};
        auto path = TestHome_.TablePath(name);
        switch (table.operation_case()) {
            case NProto::TTableSpec::kBootstrap:
                YT_LOG_INFO("Bootstrap (Name: %v)", table.name());
                Infos_.push_back(PopulateBootstrapDataset(table.bootstrap(), path));
                break;
            case NProto::TTableSpec::kMap:
                YT_LOG_INFO("Map (Source: %v, Target: %v)", table.parent(), table.name());
                Infos_.push_back(PopulateMapDataset(name, *parent, table.map(), path));
                break;
            case NProto::TTableSpec::kReduce:
                YT_LOG_INFO("Reduce (Source: %v, Target: %v)", table.parent(), table.name());
                Infos_.push_back(PopulateReduceDataset(name, *parent, table.reduce(), path));
                break;
            case NProto::TTableSpec::kSort:
                YT_LOG_INFO("Sort (Source: %v, Target: %v)", table.parent(), table.name());
                Infos_.push_back(PopulateSortDataset(name, *parent, table.sort(), path));
                break;
            case NProto::TTableSpec::OPERATION_NOT_SET:
                break;
        }
    }
}

TString TRunner::CloneTableViaMap(const TTable& table, const TString& sourcePath, const TString& targetPath)
{
    std::vector<int> allIndices;
    for (int i = 0; i < std::ssize(table.DataColumns); ++i) {
        allIndices.push_back(i);
    }

    auto identityOp = std::make_unique<TSingleMultiMapper>(
        table,
        std::make_unique<TIdentityRowMapper>(
            table,
            allIndices
        )
    );

    RunMap(Client_,
           Pool_,
           TestHome_,
           sourcePath,
           targetPath,
           table,
           table,
           *identityOp);

    OperationPtrs_.push_back(std::move(identityOp));

    return targetPath;
}

}  // namespace NYT::NTest
