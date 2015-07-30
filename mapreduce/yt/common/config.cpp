#include "config.h"

#include "log.h"

#include <library/json/json_reader.h>
#include <library/svnversion/svnversion.h>

#include <util/string/strip.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/hostname.h>
#include <util/system/user.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka TConfig::GetEnv(const char* var, const char* defaultValue)
{
    char* value = getenv(var);
    return value ? value : defaultValue;
}

bool TConfig::GetBool(const char* var)
{
    Stroka val = GetEnv(var, "");
    return val != "" && val != "0" && val != "False" && val != "FALSE";
}

int TConfig::GetInt(const char* var, int defaultValue)
{
    int result = 0;
    Stroka val = GetEnv(var, "");
    if (val.Empty()) {
        return defaultValue;
    }
    try {
        result = FromString<int>(val);
    } catch (yexception&) {
        LOG_FATAL("Cannot parse %s=%s as integer", var, ~val);
    }
    return result;
}

TDuration TConfig::GetDuration(const char* var, int defaultValueSeconds)
{
    return TDuration::Seconds(GetInt(var, defaultValueSeconds));
}

Stroka TConfig::GetEncoding(const char* var)
{
    Stroka encoding = GetEnv(var, "identity");

    const char* supportedEncodings[] = {
        "identity",
        "gzip",
        "y-lzo",
        "y-lzf",
    };

    for (size_t i = 0; i < ARRAY_SIZE(supportedEncodings); ++i) {
        if (encoding == supportedEncodings[i])
            return encoding;
    }

    LOG_FATAL("%s: encoding '%s' is not supported", var, ~encoding);

    return ""; // never gets here
}

void TConfig::LoadToken()
{
    TFsPath path(GetHomeDir() + "/.yt/token");
    if (path.IsFile()) {
        Token = Strip(TFileInput(~path).ReadAll());
    }

    Stroka envToken = GetEnv("YT_TOKEN");
    if (!envToken.empty()) {
        Token = envToken;
    }

    for (size_t i = 0; i < Token.size(); ++i) {
        char ch = Token[i];
        bool isDigit = '0' <= ch && ch <= '9';
        bool isUpper = 'A' <= ch && ch <= 'Z';
        bool isLower = 'a' <= ch && ch <= 'z';
        if (!(isDigit || isUpper || isLower)) {
            LOG_FATAL("Incorrect token character '%c' at position %" PRISZT, ch, i);
        }
    }
}

void TConfig::LoadSpec()
{
    Stroka strSpec = GetEnv("YT_SPEC", "{}");
    TStringInput input(strSpec);
    if (!NJson::ReadJsonTree(&input, &Spec, false)) {
        LOG_FATAL("YT_SPEC: Cannot parse json");
    }
    if (!Spec.IsMap()) {
        LOG_FATAL("YT_SPEC: Not a map node");
    }
}

void TConfig::LoadTimings()
{
    ConnectTimeout = GetDuration("YT_CONNECT_TIMEOUT", 10);
    SendReceiveTimeout = GetDuration("YT_SEND_RECEIVE_TIMEOUT", 60);
    PingInterval = GetDuration("YT_PING_INTERVAL", 3);
    RetryInterval = GetDuration("YT_RETRY_INTERVAL", 3);
    RateLimitExceededRetryInterval = GetDuration("YT_RATE_LIMIT_EXCEEDED_RETRY_INTERVAL", 60);
}

TConfig::TConfig()
{
    Hosts = GetEnv("YT_HOSTS", "hosts");
    Pool = GetEnv("YT_POOL");
    Prefix = GetEnv("YT_PREFIX");
    ApiVersion = GetEnv("YT_VERSION", "v3");

    ContentEncoding = GetEncoding("YT_CONTENT_ENCODING");
    AcceptEncoding = GetEncoding("YT_ACCEPT_ENCODING");

    GlobalTxId = GetEnv("YT_TRANSACTION", "");

    ForceIpV4 = GetBool("YT_FORCE_IPV4");
    ForceIpV6 = GetBool("YT_FORCE_IPV6");

    LoadToken();
    LoadSpec();
    LoadTimings();

    RetryCount = GetInt("YT_RETRY_COUNT", 10);
}

TConfig* TConfig::Get()
{
    return Singleton<TConfig>();
}

////////////////////////////////////////////////////////////////////////////////

TProcessProperties::TProcessProperties()
{
    try {
        HostName = ::HostName();
    } catch (yexception&) {
        LOG_FATAL("Cannot get host name");
    }

    try {
        UserName = ::GetUsername();
    } catch (yexception&) {
        LOG_FATAL("Cannot get user name");
    }

    Pid = static_cast<int>(getpid());

    ClientVersion = Sprintf("YT C++ client v2 r%d", GetProgramSvnRevision());
}

void TProcessProperties::SetCommandLine(int argc, const char* argv[])
{
    for (int i = 0; i < argc; ++i) {
        CommandLine.push_back(argv[i]);
    }
}

TProcessProperties* TProcessProperties::Get()
{
    return Singleton<TProcessProperties>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

