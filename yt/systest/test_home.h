#pragma once

#include <library/cpp/yt/threading/spin_lock.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <random>

namespace NYT::NTest {

// Ater the Init() call, Objects of TTestHome are safe to use from different threads concurrently.
class TTestHome
{
public:
    explicit TTestHome(IClientPtr client, const TString& homeDirectory);
    explicit TTestHome(IClientPtr client, const TString& homeDirectory, TDuration ttl);
    void Init();

    const TString& Dir() const { return Dir_; }
    const TString& CoreTable() const { return CoreTable_; }
    const TString& StderrTable() const { return StderrTable_; }
    TString ValidatorsDir() const { return Dir_ + "/validators"; }

    TString TablePath(const TString& tableName) const;
    TString CreateRandomTablePath();
    TString CreateIntervalPath(const TString& name, int index, int retrytAttempt);

private:
    const TString HomeDirectory_;
    const TDuration Ttl_;
    IClientPtr Client_;
    TString Dir_;
    TString StderrTable_, CoreTable_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::random_device RandDevice_;
    std::mt19937_64 Engine_;
    std::uniform_int_distribution<int32_t> UniformIntDistribution_;
    std::uniform_int_distribution<int16_t> UniformShortDistribution_;

    TString GenerateFullRandomId();
    TString GenerateShortRandomId();
};

}  // namespace NYT::NTest
