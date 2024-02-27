#pragma once

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/public.h>

#include <yt/systest/proto/validator.pb.h>

#include <yt/systest/config.h>
#include <yt/systest/dataset.h>
#include <yt/systest/test_home.h>
#include <yt/systest/worker_set.h>

namespace NYT::NTest {

///////////////////////////////////////////////////////////////////////////////

class TValidator
{
public:
    TValidator(
        const TString& pool,
        TValidatorConfig config,
        IClientPtr client,
        NApi::IClientPtr rpcClient,
        TTestHome& testHome);

    void Start();

    void Stop();

    TStoredDataset VerifyMap(
        const TString& targetName,
        const TString& sourcePath,
        const TString& targetPath,
        const TTable& sourceTable,
        const IMultiMapper& operation);

    TStoredDataset VerifyReduce(
        const TString& targetName,
        const TString& sourcePath,
        const TString& targetPath,
        const TTable& sourceTable,
        const TReduceOperation& reduceOperation);

    TStoredDataset VerifySort(
        const TString& targetName,
        const TString& sourcePath,
        const TString& targetPath,
        const TTable& sourceTable,
        const TSortOperation& operation);
private:

    TString Pool_;
    const TValidatorConfig Config_;
    IClientPtr Client_;
    NApi::IClientPtr RpcClient_;
    TTestHome& TestHome_;
    IOperationPtr Operation_;
    NConcurrency::IThreadPoolPtr ThreadPool_;
    TWorkerSet WorkerSet_;

    NLogging::TLogger Logger;

    struct TableIntervalInfo
    {
        int64_t RowCount = 0;
        int64_t TotalBytes = 0;
        std::vector<int64_t> Boundaries;
    };

    void StartValidatorOperation();
    std::vector<TString> PollWorkers();

    TableIntervalInfo GetMapIntervalBoundaries(const TString& tablePath, int64_t intervalBytes);

    TableIntervalInfo GetReduceIntervalBoundaries(
        const TTable& table,
        const TString& tablePath,
        int64_t intervalBytes);

    TFuture<TString> StartMapInterval(
        const TString& targetName,
        int retryAttempt,
        int intervalIndex,
        int numIntervals,
        const TString& tablePath,
        int64_t start,
        int64_t limit,
        TDuration timeout,
        const TTable& table,
        const IMultiMapper& mapper);

    TFuture<TString> StartReduceInterval(
        const TString& targetName,
        int retryAttempt,
        int intervalIndex,
        int numIntervals,
        const TString& tablePath,
        int64_t start,
        int64_t limit,
        TDuration timeout,
        const TTable& table,
        const TReduceOperation& operation);

    TFuture<TString> StartSortInterval(
        const TString& targetName,
        int retryAttempt,
        int intervalIndex,
        int numIntervals,
        const TString& tablePath,
        int64_t start,
        int64_t limit,
        TDuration timeout,
        const TTable& table,
        const TSortOperation& operation);

    TFuture<typename NRpc::TTypedClientResponse<NProto::TRspCompareInterval>::TResult>
    StartCompareInterval(
        int attempt,
        const TString& targetPath,
        const TString& intervalPath,
        int64_t start,
        int64_t limit,
        int intervalIndex,
        int numIntervals,
        TDuration timeout);

    TFuture<typename NRpc::TTypedClientResponse<NProto::TRspMergeSortedAndCompare>::TResult>
    StartMergeSortedAndCompare(
        int attempt,
        const std::vector<TString>& intervalPath,
        TDuration timeout,
        const TString& targetPath,
        const TTable& table);

    TStoredDataset CompareIntervals(
        const TString& targetPath,
        std::vector<TFuture<TString>>& intervalResult);

};

}  // namespace NYT::NTest
