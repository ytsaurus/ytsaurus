#pragma once

#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/public.h>

#include <yt/systest/dataset.h>
#include <yt/systest/test_home.h>

namespace NYT::NTest {

struct TValidatorConfig
{
    int NumJobs;
    int64_t IntervalBytes;
};

///////////////////////////////////////////////////////////////////////////////

class TValidator
{
public:
    TValidator(
        TValidatorConfig config,
        IClientPtr client,
        NApi::IClientPtr rpcClient,
        TTestHome& testHome);

    void Start();

    void Stop();

    TStoredDataset VerifyMap(
        const TString& sourcePath,
        const TString& targetPath,
        const TTable& sourceTable,
        const IMultiMapper& operation);

    TStoredDataset VerifyReduce(
        const TString& sourcePath,
        const TString& targetPath,
        const TTable& sourceTable,
        const TReduceOperation& reduceOperation);

    TStoredDataset VerifySort(
        const TString& sourcePath,
        const TString& targetPath,
        const TTable& sourceTable,
        const TSortOperation& operation);

private:
    TValidatorConfig Config_;
    IClientPtr Client_;
    NApi::IClientPtr RpcClient_;
    TTestHome& TestHome_;
    IOperationPtr Operation_;
    NConcurrency::IThreadPoolPtr ThreadPool_;

    std::atomic<bool> Stopping_;
    TPromise<void> PollerDone_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::vector<TString> Workers_;

    NLogging::TLogger Logger;

    struct TableIntervalInfo
    {
        int64_t RowCount = 0;
        int64_t TotalBytes = 0;
        std::vector<int64_t> Boundaries;
    };

    void StartValidatorOperation();
    TString GetWorker();
    void PollVanillaWorkers();

    TableIntervalInfo GetMapIntervalBoundaries(const TString& tablePath, int64_t intervalBytes);

    TableIntervalInfo GetReduceIntervalBoundaries(
        const TTable& table,
        const TString& tablePath,
        int64_t intervalBytes);

    template <typename T>
    TStoredDataset CompareIntervals(
        const TString& targetPath,
        std::vector<TFuture<T>>& intervalResult,
        const std::vector<TString>& intervalPath);

};

}  // namespace NYT::NTest
