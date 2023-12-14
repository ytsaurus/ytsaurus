#pragma once

#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT::NTest {

class TValidatorJob : public IVanillaJob<>
{
public:
    TValidatorJob() = default;

    TValidatorJob(TString dir);
    virtual void Do() override;

    Y_SAVELOAD_JOB(Dir_);

private:
    TString Dir_;
    char Hostname_[1024];
};

}  // namespace NYT::NTest
