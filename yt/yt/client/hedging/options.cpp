#include "options.h"

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/env.h>

namespace NYT::NClient::NHedging {

NYT::NApi::TClientOptions GetClientOpsFromEnv() {
    NYT::NApi::TClientOptions options;

    auto user = Strip(GetEnv("YT_USER"));
    if (!user.empty()) {
        options.User = user;
    }

    auto token = Strip(GetEnv("YT_TOKEN"));
    if (!token.empty()) {
        options.Token = token;
    } else {
        auto tokenPath = Strip(GetEnv("YT_TOKEN_PATH"));
        if (tokenPath.empty()) {
            tokenPath = GetHomeDir() + "/.yt/token";
        }
        TFsPath path(tokenPath);
        if (path.IsFile()) {
            options.Token = Strip(TIFStream(path).ReadAll());
        }
    }
    Y_ENSURE(options.Token && !options.Token->empty() , "No YtToken found!");
    return options;
}

const NYT::NApi::TClientOptions& GetClientOpsFromEnvStatic() {
    static const NYT::NApi::TClientOptions options = GetClientOpsFromEnv();
    return options;
}

} // namespace NYT::NClient::NHedging
