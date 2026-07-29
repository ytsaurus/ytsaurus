#pragma once

#include <yt/yt/core/test_framework/framework.h>

#include <util/system/env.h>

namespace NYT::NFlow::NCompanionServer::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Saves and restores the companion environment variables around each test.
class TCompanionEnvGuardTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        SavedMode_ = GetEnv("YT_FLOW_MODE");
        SavedConfig_ = GetEnv("YT_FLOW_COMPANION_CONFIG");
    }

    void TearDown() override
    {
        SetEnv("YT_FLOW_MODE", SavedMode_);
        SetEnv("YT_FLOW_COMPANION_CONFIG", SavedConfig_);
    }

private:
    TString SavedMode_;
    TString SavedConfig_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer::NTesting
