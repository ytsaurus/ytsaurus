#pragma once

#include <yt/cpp/mapreduce/interface/client.h>

#include <random>

namespace NYT::NTest {

class TTestHome
{
public:
    explicit TTestHome(IClientPtr client, const TString& homeDirectory);
    void Init();

    const TString& Dir() const { return Dir_; }
    const TString& CoreTable() const { return CoreTable_; }
    const TString& StderrTable() const { return StderrTable_; }
    TString ValidatorsDir() const { return Dir_ + "/validators"; }

    TString TablePath(const TString& tableName);
    TString CreateRandomTablePath();
    TString CreateIntervalPath(const TString& name, int index);

private:
    const TString HomeDirectory_;
    IClientPtr Client_;
    TString Dir_;
    TString StderrTable_, CoreTable_;

    std::random_device RandDevice_;
    std::mt19937 Engine_;
    std::uniform_int_distribution<int32_t> UniformIntDistribution_;
    std::uniform_int_distribution<int16_t> UniformShortDistribution_;

    TString GenerateFullRandomId();
    TString GenerateShortRandomId();
};

}  // namespace NYT::NTest
