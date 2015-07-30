#pragma once

#include <library/json/json_value.h>

#include <util/generic/stroka.h>
#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TConfig
{
    Stroka Hosts;
    Stroka Pool;
    Stroka Token;
    Stroka Prefix;
    Stroka ApiVersion;

    Stroka ContentEncoding;
    Stroka AcceptEncoding;

    Stroka GlobalTxId;

    bool ForceIpV4;
    bool ForceIpV6;

    NJson::TJsonValue Spec;

    TDuration ConnectTimeout;
    TDuration SendReceiveTimeout;
    TDuration PingInterval;
    TDuration RetryInterval;
    TDuration RateLimitExceededRetryInterval;

    int RetryCount;

    static Stroka GetEnv(const char* var, const char* defaultValue = "");
    static bool GetBool(const char* var);
    static int GetInt(const char* var, int defaultValue);
    static TDuration GetDuration(const char* var, int defaultValueSeconds);
    static Stroka GetEncoding(const char* var);

    void LoadToken();
    void LoadSpec();
    void LoadTimings();

    TConfig();

    static TConfig* Get();
};

////////////////////////////////////////////////////////////////////////////////

struct TProcessProperties
{
    Stroka HostName;
    Stroka UserName;
    yvector<Stroka> CommandLine;
    int Pid;
    Stroka ClientVersion;

    TProcessProperties();

    void SetCommandLine(int argc, const char* argv[]);

    static TProcessProperties* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

