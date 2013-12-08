#include "stdafx.h"
#include "framework.h"

#include <core/misc/address.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/rpc/dispatcher.h>

#include <core/logging/log_manager.h>

#include <core/profiling/profiling_manager.h>

#include <ytlib/meta_state/async_change_log.h>

#include <core/concurrency/delayed_executor.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <util/string/printf.h>
#include <util/string/escape.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv)
{
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    testing::InitGoogleTest(&argc, argv);
    int rv = RUN_ALL_TESTS();

    // TODO(sandello): Refactor this.
    // XXX(sandello): Keep in sync with...
    //   server/main.cpp
    //   driver/main.cpp
    //   unittests/utmain.cpp
    //   nodejs/src/common.cpp
    //   ../python/yt/bindings/shutdown.cpp
    // Feel free to add your cpp here. Welcome to the Shutdown Club!

    NYT::NMetaState::TAsyncChangeLog::Shutdown();
    NYT::NChunkClient::TDispatcher::Get()->Shutdown();
    NYT::NRpc::TDispatcher::Get()->Shutdown();
    NYT::NBus::TTcpDispatcher::Get()->Shutdown();
    NYT::NConcurrency::TDelayedExecutor::Shutdown();
    NYT::NProfiling::TProfilingManager::Get()->Shutdown();
    NYT::TAddressResolver::Get()->Shutdown();
    NYT::NLog::TLogManager::Get()->Shutdown();

    return rv;
}

////////////////////////////////////////////////////////////////////////////////
