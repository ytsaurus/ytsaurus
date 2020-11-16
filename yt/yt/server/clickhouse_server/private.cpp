#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// See comments around declaration of these objects in private.h.

const NLogging::TLogger ClickHouseYtLogger("ClickHouseYT");
const NProfiling::TRegistry ClickHouseYtProfiler("/clickhouse/yt");

const NLogging::TLogger ClickHouseNativeLogger("ClickHouseNative");
const NProfiling::TRegistry ClickHouseNativeProfiler("/clickhouse/native");

const TString CacheUserName("yt-clickhouse-cache");

const TString InternalRemoteUserName("$remote");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

// This is an override of weak symbol from Common/Allocator.h.
// We do not want ClickHouse allocator to use raw mmaps as ytalloc already
// does that by himself.
__attribute__((__used__)) extern const size_t MMAP_THRESHOLD = static_cast<size_t>(1) << 60;
