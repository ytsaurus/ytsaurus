#include "driver_factory.h"

#include <util/system/compiler.h>

namespace NYT::NIOTest {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TConfigurationPtr configuration)
{
    auto init = [&] (TDriverOptionsBase& basicOptions) {
        basicOptions.Start = configuration->Start;
        basicOptions.MaxBlockSize = (size_t) 1 << configuration->BlockSizeLog;
        basicOptions.Validate = configuration->Validate;
    };

    switch (configuration->Driver.Type) {
        case EDriverType::Memcpy: {
            TMemcpyDriverOptions options;
            init(options);
            return CreateMemcpyDriver(options);
        }

        case EDriverType::Rw: {
            TPrwDriverOptions options;
            init(options);
            options.Files = configuration->Files;
            return CreatePrwDriver(options);
        }

        case EDriverType::Prwv2: {
            TPrwv2DriverOptions options;
            init(options);
            options.Files = configuration->Files;
            options.Config = ConvertTo<TPrwv2DriverConfigPtr>(configuration->Driver.Config);
            return CreatePrwv2Driver(options);
        }

        case EDriverType::Async: {
            TAsyncDriverOptions options;
            init(options);
            options.Files = configuration->Files;
            options.Config = ConvertTo<TAsyncDriverConfigPtr>(configuration->Driver.Config);
            return CreateAsyncDriver(options);
        }

        case EDriverType::Uring: {
            TUringDriverOptions options;
            init(options);
            options.Files = configuration->Files;
            options.Config = ConvertTo<TUringDriverConfigPtr>(configuration->Driver.Config);
            return CreateUringDriver(options);
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
