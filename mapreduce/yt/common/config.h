#pragma once

#include <mapreduce/yt/interface/node.h>

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
    Stroka LogLevel;

    Stroka ContentEncoding;
    Stroka AcceptEncoding;

    Stroka GlobalTxId;

    bool ForceIpV4;
    bool ForceIpV6;
    bool UseHosts;

    TNode Spec;

    TDuration ConnectTimeout;
    TDuration SocketTimeout;
    TDuration TxTimeout;
    TDuration PingTimeout;
    TDuration PingInterval;
    TDuration RetryInterval;
    TDuration RateLimitExceededRetryInterval;
    TDuration StartOperationRetryInterval;

    int RetryCount;
    int StartOperationRetryCount;

    Stroka RemoteTempFilesDirectory;
    Stroka RemoteTempTablesDirectory;

    // common wrapper

    TDuration TxClientTimeout;
    TDuration TxOperationTimeout;

    enum EOrderGuarantees {
        // Each mode implies the ones preceding it
        OG_STANDARD,          // standard Map-Reduce
        OG_TESTABLE,          // values are taken into account for resolving ties (sorting by KSV)
        OG_STRICTLY_TESTABLE, // sorted merge and write operations expect KSV-sorted input, may require preparing external tables
    };
    EOrderGuarantees OrderGuarantees;

    bool DisableClientSubTransactions;
    bool CreateTablesUnderTransaction;


    static Stroka GetEnv(const char* var, const char* defaultValue = "");
    static bool GetBool(const char* var, bool defaultValue = false);
    static int GetInt(const char* var, int defaultValue);
    static TDuration GetDuration(const char* var, TDuration defaultValue);
    static Stroka GetEncoding(const char* var);

    static void ValidateToken(const Stroka& token);
    static TNode LoadJsonSpec(const Stroka& strSpec);

    void LoadToken();
    void LoadSpec();
    void LoadTimings();

    TConfig();

    static TConfig* Get();
};

////////////////////////////////////////////////////////////////////////////////

struct TProcessState
{
    Stroka HostName;
    Stroka UserName;
    yvector<Stroka> CommandLine;
    int Pid;
    Stroka ClientVersion;

    TProcessState();

    void SetCommandLine(int argc, const char* argv[]);

    static TProcessState* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

