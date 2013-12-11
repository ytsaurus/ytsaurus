#include "stdafx.h"
#include "framework.h"

#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TEST(TLoggingTest, ReloadsOnSigHup)
{
    NLog::TLogger Logger("Test");

    LOG_INFO("Prepaing logging thread");
    sleep(1); // In sleep() we trust.

    int version = NLog::TLogManager::Get()->GetConfigVersion();
    int revision = NLog::TLogManager::Get()->GetConfigRevision();

    kill(getpid(), SIGHUP);

    LOG_INFO("Awaking logging thread");
    sleep(1); // In sleep() we trust.

    int newVersion = NLog::TLogManager::Get()->GetConfigVersion();
    int newRevision = NLog::TLogManager::Get()->GetConfigRevision();

    EXPECT_EQ(version, newVersion);
    EXPECT_NE(revision, newRevision);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
