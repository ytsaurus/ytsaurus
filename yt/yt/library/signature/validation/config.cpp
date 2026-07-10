#include "config.h"

#include <yt/yt/client/api/client_common.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NYPath;
using namespace NYTree;

static constexpr TYPathBuf DefaultKeyPath = "//sys/public_keys/by_owner";
static constexpr i64 DefaultKeyCacheCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

void TCypressKeyReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default(TYPath(DefaultKeyPath))
        .NonEmpty();

    registrar.Parameter("cypress_read_options", &TThis::CypressReadOptions)
        .DefaultCtor([] {
            auto options = New<TSerializableMasterReadOptions>();
            options->ReadFrom = EMasterChannelKind::ClientSideCache;
            options->ExpireAfterSuccessfulUpdateTime = TDuration::Hours(12);
            return options;
        });

    registrar.Parameter("key_cache", &TThis::KeyCache)
        .DefaultCtor([] {
            return TSlruCacheConfig::CreateWithCapacity(DefaultKeyCacheCapacity);
        });
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureValidationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_key_reader", &TThis::CypressKeyReader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
