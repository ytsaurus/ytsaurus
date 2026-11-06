#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

namespace NYql {

void YtReopenLog() {
    NYT::NLogging::TLogManager::Get()->Reopen();
}

void YtInitLogs(const TString& logFile) {
    auto logManager = NYT::NLogging::TLogManager::Get();

    TString logConfig = Sprintf(" \
        { \
            \"rules\" = [ \
                { \
                    \"min_level\" = \"info\"; \
                    \"writers\" = [ \
                        \"yt_rpc_log\"; \
                    ]; \
                    \"exclude_categories\" = [ \
                        \"Bus\"; \
                    ]; \
                }; \
            ]; \
            \"writers\" = { \
                \"yt_rpc_log\" = { \
                    \"type\" = \"file\"; \
                    \"file_name\" = \"%s\"; \
                } \
            } \
        }", logFile.c_str());

    auto ytLogConfigNode = NYT::NYTree::ConvertTo<NYT::NYTree::INodePtr>(
        NYT::NYson::TYsonString(logConfig));
    auto logManagerConfig = NYT::New<NYT::NLogging::TLogManagerConfig>();
    logManagerConfig->Load(ytLogConfigNode);
    logManager->Configure(logManagerConfig);
}

void YtShutdown() {
    NYT::Shutdown();
}

} // namespace NYql
