#include "config.h"

#include "log.h"
#include "node_builder.h"
#include "helpers.h"

#include <library/json/json_reader.h>
#include <library/svnversion/svnversion.h>

#include <util/string/strip.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/generic/singleton.h>
#include <util/string/cast.h>
#include <util/string/type.h>
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

bool TConfig::GetBool(const char* var, bool defaultValue)
{
    Stroka val = GetEnv(var, "");
    if (val.Empty()) {
        return defaultValue;
    }
    return IsTrue(val);
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

TDuration TConfig::GetDuration(const char* var, TDuration defaultValue)
{
    return TDuration::Seconds(GetInt(var, defaultValue.Seconds()));
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

    for (size_t i = 0; i < Y_ARRAY_SIZE(supportedEncodings); ++i) {
        if (encoding == supportedEncodings[i])
            return encoding;
    }

    LOG_FATAL("%s: encoding '%s' is not supported", var, ~encoding);

    return ""; // never gets here
}

void TConfig::ValidateToken(const Stroka& token)
{
    for (size_t i = 0; i < token.size(); ++i) {
        ui8 ch = token[i];
        if (ch < 0x21 || ch > 0x7e) {
            LOG_FATAL("Incorrect token character '%c' at position %" PRISZT, ch, i);
        }
    }
}

Stroka TConfig::LoadTokenFromFile(const Stroka& tokenPath)
{
    TFsPath path(tokenPath);
    return path.IsFile() ? Strip(TFileInput(~path).ReadAll()) : Stroka();
}

TNode TConfig::LoadJsonSpec(const Stroka& strSpec)
{
    TNode spec;
    TStringInput input(strSpec);
    TNodeBuilder builder(&spec);
    TYson2JsonCallbacksAdapter callbacks(&builder);

    if (!NJson::ReadJson(&input, &callbacks)) {
        LOG_FATAL("Cannot parse json spec");
    }

    if (!spec.IsMap()) {
        LOG_FATAL("Json spec is not a map");
    }
    return spec;
}

void TConfig::LoadToken()
{
    Stroka envToken = GetEnv("YT_TOKEN");
    if (envToken) {
        Token = envToken;
    } else {
        Stroka tokenPath = GetEnv("YT_TOKEN_PATH");
        if (!tokenPath) {
            tokenPath = GetHomeDir() + "/.yt/token";
        }
        Token = LoadTokenFromFile(tokenPath);
    }
    ValidateToken(Token);
}

void TConfig::LoadSpec()
{
    Stroka strSpec = GetEnv("YT_SPEC", "{}");
    Spec = LoadJsonSpec(strSpec);

    strSpec = GetEnv("YT_TABLE_WRITER", "{}");
    TableWriter = LoadJsonSpec(strSpec);
}

void TConfig::LoadTimings()
{
    ConnectTimeout = GetDuration("YT_CONNECT_TIMEOUT",
        TDuration::Seconds(10));

    SocketTimeout = GetDuration("YT_SOCKET_TIMEOUT",
        GetDuration("YT_SEND_RECEIVE_TIMEOUT", // common
            TDuration::Seconds(60)));

    TxTimeout = GetDuration("YT_TX_TIMEOUT",
        TDuration::Seconds(120));

    PingTimeout = GetDuration("YT_PING_TIMEOUT",
        TDuration::Seconds(5));

    PingInterval = GetDuration("YT_PING_INTERVAL",
        TDuration::Seconds(5));

    RetryInterval = GetDuration("YT_RETRY_INTERVAL",
        TDuration::Seconds(3));

    RateLimitExceededRetryInterval = GetDuration("YT_RATE_LIMIT_EXCEEDED_RETRY_INTERVAL",
        TDuration::Seconds(60));

    StartOperationRetryInterval = GetDuration("YT_START_OPERATION_RETRY_INTERVAL",
        TDuration::Seconds(60));

    TxClientTimeout = GetDuration("YT_CLIENT_TIMEOUT", // common
        TDuration::Seconds(60));

    TxOperationTimeout = GetDuration("YT_OPERATION_TIMEOUT", // common
        TDuration::Seconds(180));
}

TConfig::TConfig()
{
    Hosts = GetEnv("YT_HOSTS", "hosts");
    Pool = GetEnv("YT_POOL");
    Prefix = GetEnv("YT_PREFIX");
    ApiVersion = GetEnv("YT_VERSION", "v3");
    LogLevel = GetEnv("YT_LOG_LEVEL", "error");

    ContentEncoding = GetEncoding("YT_CONTENT_ENCODING");
    AcceptEncoding = GetEncoding("YT_ACCEPT_ENCODING");

    GlobalTxId = GetEnv("YT_TRANSACTION", "");

    ForceIpV4 = GetBool("YT_FORCE_IPV4");
    ForceIpV6 = GetBool("YT_FORCE_IPV6");
    UseHosts = GetBool("YT_USE_HOSTS", true);

    LoadToken();
    LoadSpec();
    LoadTimings();

    RetryCount = GetInt("YT_RETRY_COUNT", 10);
    StartOperationRetryCount = GetInt("YT_START_OPERATION_RETRY_COUNT", 30);

    RemoteTempFilesDirectory = GetEnv("YT_FILE_STORAGE",
        "//tmp/yt_wrapper/file_storage");
    RemoteTempTablesDirectory = GetEnv("YT_TEMP_TABLES_STORAGE",
        "//tmp/yt_wrapper/table_storage");

    JobBinary = GetEnv("YT_JOB_BINARY");

    UseClientProtobuf = GetBool("YT_USE_CLIENT_PROTOBUF", true);

    MountSandboxInTmpfs = GetBool("YT_MOUNT_SANDBOX_IN_TMPFS");

    ConnectionPoolSize = GetInt("YT_CONNECTION_POOL_SIZE", 16);

    OrderGuarantees = GetBool("YT_STRICTLY_TESTABLE") // common
        ? OG_STRICTLY_TESTABLE
        : GetBool("YT_TESTABLE") ? OG_TESTABLE : OG_STANDARD;

    DisableClientSubTransactions = GetBool("YT_DISABLE_CLIENT_SUB_TRANSACTIONS"); // common
    CreateTablesUnderTransaction = GetBool("YT_CREATE_TABLES_UNDER_TRANSACTION", true); // common
}

TConfig* TConfig::Get()
{
    return Singleton<TConfig>();
}

////////////////////////////////////////////////////////////////////////////////

TProcessState::TProcessState()
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

    if (!ClientVersion) {
        ClientVersion = Sprintf("YT C++ native r%d", GetProgramSvnRevision());
    }
}

void TProcessState::SetCommandLine(int argc, const char* argv[])
{
    for (int i = 0; i < argc; ++i) {
        CommandLine.push_back(argv[i]);
    }
}

TProcessState* TProcessState::Get()
{
    return Singleton<TProcessState>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

