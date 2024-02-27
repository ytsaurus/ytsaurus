#pragma once

#include <library/cpp/yt/threading/spin_lock.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/systest/config.h>

#include <random>

namespace NYT::NTest {

// Ater the Init() call, Objects of TTestHome are safe to use from different threads concurrently.
class TTestHome
{
public:
    explicit TTestHome(IClientPtr client, const THomeConfig& homeConfig);

    void Init();

    const TString& Dir() const { return Dir_; }
    const TString& CoreTable() const { return CoreTable_; }
    TString StderrTable(const TString& operationName);
    TString ValidatorsDir() const { return Dir_ + "/validators"; }
    TString IntervalsDir(int shard) const;

    TString TablePath(const TString& tableName) const;

    TString CreateIntervalPath(const TString& name, int index, int retryAttempt);

private:
    const THomeConfig Config_;
    IClientPtr Client_;
    TString Dir_;
    TString CoreTable_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::random_device RandDevice_;
    std::mt19937_64 Engine_;
    std::uniform_int_distribution<int32_t> UniformShardDistribution_;
    std::uniform_int_distribution<int32_t> UniformIntDistribution_;
    std::uniform_int_distribution<int16_t> UniformShortDistribution_;

    TString GenerateFullRandomId();
    TString GenerateShortRandomId();
};

}  // namespace NYT::NTest
