#pragma once

#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <DBPoco/Logger.h>
#include <DBPoco/Net/HTTPRequest.h>
#include <DBPoco/URI.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <Common/BridgeProtocolVersion.h>

namespace DB
{

// Common base class to access the clickhouse-library-bridge.
class LibraryBridgeHelper : public IBridgeHelper
{
protected:
    explicit LibraryBridgeHelper(ContextPtr context_);

    void startBridge(std::unique_ptr<ShellCommand> cmd) const override;

    String serviceAlias() const override { return "clickhouse-library-bridge"; }

    String serviceFileName() const override { return serviceAlias(); }

    unsigned getDefaultPort() const override { return DEFAULT_PORT; }

    bool startBridgeManually() const override { return false; }

    String configPrefix() const override { return "library_bridge"; }

    const DBPoco::Util::AbstractConfiguration & getConfig() const override { return config; }

    LoggerPtr getLog() const override { return log; }

    DBPoco::Timespan getHTTPTimeout() const override { return http_timeout; }

    DBPoco::URI createBaseURI() const override;

    static constexpr size_t DEFAULT_PORT = 9012;

    const DBPoco::Util::AbstractConfiguration & config;
    LoggerPtr log;
    const DBPoco::Timespan http_timeout;
    std::string bridge_host;
    size_t bridge_port;
    ConnectionTimeouts http_timeouts;
    DBPoco::Net::HTTPBasicCredentials credentials{};
};

}
