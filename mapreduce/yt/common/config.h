#pragma once

#include <mapreduce/yt/interface/node.h>
#include <mapreduce/yt/interface/common.h>

#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NYT {

enum EEncoding : int {
    E_IDENTITY  /* "identity" */,
    E_GZIP      /* "gzip" */,
    E_Y_LZO     /* "y-lzo" */,
    E_Y_LZF     /* "y-lzf" */
};

////////////////////////////////////////////////////////////////////////////////

struct TConfig
{
    TString Hosts;
    TString Pool;
    TString Token;
    TString Prefix;
    TString ApiVersion;
    TString LogLevel;

    EEncoding ContentEncoding;
    EEncoding AcceptEncoding;

    TString GlobalTxId;

    bool ForceIpV4;
    bool ForceIpV6;
    bool UseHosts;

    TNode Spec;
    TNode TableWriter;

    TDuration ConnectTimeout;
    TDuration SocketTimeout;
    TDuration TxTimeout;
    TDuration PingTimeout;
    TDuration PingInterval;

    // How often should we poll for lock state
    TDuration WaitLockPollInterval;

    TDuration RetryInterval;
    TDuration ChunkErrorsRetryInterval;

    TDuration RateLimitExceededRetryInterval;
    TDuration StartOperationRetryInterval;

    int RetryCount;
    int ReadRetryCount;
    int StartOperationRetryCount;

    TString RemoteTempFilesDirectory;
    TString RemoteTempTablesDirectory;

    TString JobBinary;

    bool UseClientProtobuf;

    int ConnectionPoolSize;

    bool MountSandboxInTmpfs;

    // common wrapper

    TDuration TxClientTimeout;
    TDuration TxOperationTimeout;

    enum EOrderGuarantees {
        // Each mode implies the ones preceding it
        OG_STANDARD,          // standard Map-Reduce
        OG_TESTABLE,          // values are taken into account for resolving ties (sorting by KSV)
        // keeps invariant: all KS-sorted in OG_STANDARD tables are KSV-sorted in this mode.
        // may require preparing external tables
        OG_STRICTLY_TESTABLE,
    };
    EOrderGuarantees OrderGuarantees;

    bool DisableClientSubTransactions;
    bool CreateTablesUnderTransaction;

    // Testing options, should never be used in user programs.
    bool UseAbortableResponse = false;
    bool EnableDebugMetrics = false;

    static bool GetBool(const char* var, bool defaultValue = false);
    static int GetInt(const char* var, int defaultValue);
    static TDuration GetDuration(const char* var, TDuration defaultValue);
    static EEncoding GetEncoding(const char* var);

    static void ValidateToken(const TString& token);
    static TString LoadTokenFromFile(const TString& tokenPath);

    static TNode LoadJsonSpec(const TString& strSpec);

    void LoadToken();
    void LoadSpec();
    void LoadTimings();

    TConfig();

    static TConfig* Get();
};

////////////////////////////////////////////////////////////////////////////////

struct TProcessState
{
    TString HostName;
    TString UserName;
    yvector<TString> CommandLine;
    int Pid;
    TString ClientVersion;

    TProcessState();

    void SetCommandLine(int argc, const char* argv[]);

    static TProcessState* Get();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

