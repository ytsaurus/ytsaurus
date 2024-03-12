
#include <library/cpp/yt/logging/logger.h>

#include <yt/systest/bootstrap_dataset.h>
#include <yt/systest/dataset.h>
#include <yt/systest/dataset_operation.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/retrier.h>
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
    int runnerThreads,
    TTestHome& testHome,
    TValidator& validator)
    : Logger("test")
    , Pool_(pool)
    , TestSpec_(testSpec)
    , Client_(client)
    , RpcClient_(rpcClient)
    , ThreadPool_(NConcurrency::CreateThreadPool(runnerThreads, "runner"))
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
        bootstrapDataset->table_schema(),
        bootstrapInfo
    };

    return result;
}

TRunner::TDatasetInfo TRunner::PopulateMapDataset(
    const TString& name,
    const TDatasetInfo& parent,
    const NProto::TMapRunSpec& runSpec)
{
    const auto& input = parent.Table;
    auto mapOperation = CreateFromProto(input, runSpec.operation());

    TTable table = CreateTableFromMapOperation(*mapOperation);

    TString path;
    if (mapOperation->Alterable()) {
        path = TestHome_.TablePath(name, 0);
        CloneTableViaMap(parent.Table, parent.Stored.Path, path);
        AlterTable(RpcClient_, path, table);
    } else {
        TRetrier<TString> retrier([=, this, &parent, &table, &mapOperation](int attempt) {
            auto attemptPath = TestHome_.TablePath(name, attempt);

            return BIND([&]() {
                YT_LOG_INFO("Performing Map (InputTable: %v, OutputTable: %v)", parent.Stored.Path, attemptPath);

                RunMap(
                    Client_,
                    Pool_,
                    parent.Stored.Path,
                    attemptPath,
                    parent.Table,
                    table,
                    *mapOperation);
                return attemptPath;
            }).AsyncVia(GetSyncInvoker()).Run();

        });
        path = NConcurrency::WaitFor(retrier.Run()).ValueOrThrow();
    }

    auto stored = Validator_.VerifyMap(
        name,
        parent.Stored.Path,
        path,
        input,
        *mapOperation);

    TDatasetInfo result{
        table,
        stored
    };

    OperationPtrs_.push_back(std::move(mapOperation));

    return result;
}

TRunner::TDatasetInfo TRunner::PopulateReduceDataset(
    const TString& name,
    const TDatasetInfo& parent,
    const NProto::TReduceRunSpec& runSpec)
{
    std::vector<TString> columns(runSpec.reduce_by().begin(), runSpec.reduce_by().end());

    TReduceOperation reduceOperation{
        CreateFromProto(parent.Table, runSpec.operation()),
        columns
    };

    std::vector<int> reduceByIndices;
    TTable table = CreateTableFromReduceOperation(parent.Table, reduceOperation, &reduceByIndices);

    TRetrier<TString> retrier([=, this, &parent, &table, &columns, &reduceOperation](int attempt) {
        TString attemptPath = TestHome_.TablePath(name, attempt);

        return BIND([&]() {
            YT_LOG_INFO("Performing reduce (InputTable: %v, Columns: %v, OutputTable: %v)",
                parent.Stored.Path, JoinSeq(",", columns), attemptPath);
            RunReduce(Client_,
                      Pool_,
                      parent.Stored.Path,
                      attemptPath,
                      parent.Table,
                      table,
                      reduceOperation);

            return attemptPath;
        }).AsyncVia(GetSyncInvoker()).Run();
    });

    TString path = NConcurrency::WaitFor(retrier.Run()).ValueOrThrow();

    auto stored = Validator_.VerifyReduce(
        name,
        parent.Stored.Path,
        path,
        parent.Table,
        reduceOperation);


    TDatasetInfo result{
        table,
        stored
    };

    OperationPtrs_.push_back(std::move(reduceOperation.Reducer));

    return result;
}

TRunner::TDatasetInfo TRunner::PopulateSortDataset(
    const TString& name,
    const TDatasetInfo &parent,
    const NProto::TSortRunSpec& sort)
{
    std::vector<TString> columns;
    for (const auto& column : sort.sort_by()) {
        columns.push_back(column);
    }
    TSortOperation sortOperation{columns};

    TString sortColumnsString = JoinSeq(",", columns);

    TTable table;
    ApplySortOperation(parent.Table, sortOperation, &table);

    TRetrier<TString> retrier([=, this, &parent](int attempt) {
        TString attemptPath = TestHome_.TablePath(name, attempt);

        return BIND([&]() {
            YT_LOG_INFO("Performing sort (InputTable: %v, Columns: %v, OutputTable: %v)", parent.Stored.Path, sortColumnsString);
            RunSort(Client_, Pool_, parent.Stored.Path, attemptPath,
                TSortColumns(TVector<TString>(columns.begin(), columns.end())));
            return attemptPath;
        }).AsyncVia(GetSyncInvoker()).Run();
    });

    TString path = NConcurrency::WaitFor(retrier.Run()).ValueOrThrow();

    auto stored = Validator_.VerifySort(
        name,
        parent.Stored.Path,
        path,
        parent.Table,
        sortOperation);

    TDatasetInfo result{
        table,
        stored
    };

    return result;
}

void TRunner::Run()
{
    const int numTables = TestSpec_.table_size();
    Children_.resize(numTables);
    for (int i = 0; i < numTables; ++i) {
        const auto& table = TestSpec_.table(i);
        NameIndex_[table.name()] = i;
    }
    for (int i = 0; i < numTables; ++i) {
        const auto& table = TestSpec_.table(i);
        if (!table.has_parent()) {
            continue;
        }
        auto pos = NameIndex_.find(table.parent());
        if (pos == NameIndex_.end()) {
            THROW_ERROR_EXCEPTION("Unknown parent table %v for table %v", table.parent(), table.name());
        }
        Children_[pos->second].push_back(NameIndex_[table.name()]);
    }

    TableDone_.resize(numTables);
    for (int i = 0; i < numTables; ++i) {
        TableDone_[i] = NewPromise<void>();
    }

    Infos_.resize(numTables);

    std::vector<TFuture<void>> tableDone;
    for (int i = 0; i < numTables; ++i) {
        const auto& table = TestSpec_.table(i);
        if (table.has_parent()) {
            continue;
        }
        ThreadPool_->GetInvoker()->Invoke(BIND(&TRunner::PopulateTable, this, i));
    }
    for (int i = 0; i < numTables; ++i) {
        tableDone.push_back(TableDone_[i].ToFuture());
    }

    AllSucceeded(std::move(tableDone)).Get().ThrowOnError();
}

void TRunner::PopulateTable(int index)
{
    const auto& table = TestSpec_.table(index);
    YT_LOG_INFO("Starting to populate table (Index: %v, Name: %v, Spec: %v)",
        index, table.name(), table.DebugString());

    const TDatasetInfo* parent = nullptr;
    if (table.has_parent()) {
        auto pos = NameIndex_.find(table.parent());
        if (pos == NameIndex_.end()) {
            THROW_ERROR_EXCEPTION("Unknown parent table %v (current table %v)", table.parent(), table.name());
        }
        parent = &Infos_[pos->second];
    }
    TString name{table.name()};
    switch (table.operation_case()) {
        case NProto::TTableSpec::kBootstrap: {
            YT_LOG_INFO("Bootstrap (Name: %v)", table.name());
            TString path = TestHome_.TablePath(table.name(), 0);
            Infos_[index] = PopulateBootstrapDataset(table.bootstrap(), path);
            break;
        }
        case NProto::TTableSpec::kMap:
            YT_LOG_INFO("Map (Source: %v, Target: %v)", table.parent(), table.name());
            Infos_[index] = PopulateMapDataset(name, *parent, table.map());
            break;
        case NProto::TTableSpec::kReduce:
            YT_LOG_INFO("Reduce (Source: %v, Target: %v)", table.parent(), table.name());
            Infos_[index] = PopulateReduceDataset(name, *parent, table.reduce());
            break;
        case NProto::TTableSpec::kSort:
            YT_LOG_INFO("Sort (Source: %v, Target: %v)", table.parent(), table.name());
            Infos_[index] = PopulateSortDataset(name, *parent, table.sort());
            break;
        case NProto::TTableSpec::OPERATION_NOT_SET:
            break;
    }

    for (int child : Children_[index]) {
        ThreadPool_->GetInvoker()->Invoke(BIND(&TRunner::PopulateTable, this, child));
    }
    TableDone_[index].Set();
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
           sourcePath,
           targetPath,
           table,
           table,
           *identityOp);

    OperationPtrs_.push_back(std::move(identityOp));

    return targetPath;
}

}  // namespace NYT::NTest
