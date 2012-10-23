#include "stdafx.h"

#include <yt/ytlib/logging/log_manager.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#if 0
#ifndef _win_

TEST(TLoggingTest, ReloadsOnSigHup)
{
    int version = NLog::TLogManager::Get()->GetConfigVersion();
    int revision = NLog::TLogManager::Get()->GetConfigRevision();

    kill(getpid(), SIGHUP);
    sleep(2); // In sleep() we trust.

    int newVersion = NLog::TLogManager::Get()->GetConfigVersion();
    int newRevision = NLog::TLogManager::Get()->GetConfigRevision();

    EXPECT_EQ(version, newVersion);
    EXPECT_NE(revision, newRevision);
}

#endif
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
