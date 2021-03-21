#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/private.h>

namespace NYT {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

template <>
void PrintTo(const TIntrusivePtr<NChunkClient::TInputChunk>& chunk, std::ostream* os)
{
    *os << ToString(chunk->GetChunkId());
}

////////////////////////////////////////////////////////////////////////////////

namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

TLogger GetTestLogger()
{
    const auto* testInfo =
        testing::UnitTest::GetInstance()->current_test_info();

    return ChunkPoolLogger
        .WithTag("OperationId: %v, Name: %v::%v", TGuid::Create(), testInfo->name(), testInfo->test_suite_name())
        .WithMinLevel(ELogLevel::Trace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
