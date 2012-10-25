#include "stdafx.h"

#include <yt/ytlib/logging/log_manager.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TEST(TLoggingTest, ReloadsOnSigHup)
{
    int version = NLog::TLogManager::Get()->GetConfigVersion();
    int revision = NLog::TLogManager::Get()->GetConfigRevision();

    kill(getpid(), SIGHUP);

    LOG_INFO("Awaking logging thread.");
    sleep(1); // In sleep() we trust.
    LOG_INFO("Awaking logging thread.");
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
