#include <yt/yt/library/program/program.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/dns/dns_resolver.h>
#include <yt/yt/core/dns/ares_dns_resolver.h>
#include <yt/yt/core/dns/config.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/misc/optional.h>

#include <ares.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

namespace NYT {

using namespace NNet;
using namespace NDns;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDnsTestProgram
    : public TProgram
{
public:
    TDnsTestProgram()
    {
        Opts_
            .AddLongOption("retries", "DNS resolve retries")
            .StoreResult(&Retries);
        Opts_
            .AddLongOption("resolve-timeout", "DNS resolve timeout")
            .StoreResult(&ResolveTimeout);
        Opts_
            .AddLongOption("max-resolve-timeout", "DNS resolve timeout (max)")
            .StoreResult(&MaxResolveTimeout);
        Opts_
            .AddLongOption("warning-timeout", "DNS warning timeout")
            .StoreResult(&WarningTimeout);
        Opts_
            .AddLongOption("max-concurrency", "Maximum number of simultaneous DNS resolution requests")
            .StoreResult(&MaxConcurrency);
        Opts_
            .AddLongOption("jitter", "Jitter")
            .StoreResult(&Jitter);
        Opts_
            .AddLongOption("disable-jitter", "Do not use jitter at all")
            .StoreTrue(&DisableJitter);
        Opts_.AddCharOption('4', "Enable IPv4 resolution").SetFlag(&EnableIPv4);
        Opts_.AddCharOption('6', "Enable IPv6 resolution").SetFlag(&EnableIPv6);
        Opts_.SetFreeArgsMin(0);
        Opts_.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);
        Opts_.SetFreeArgTitle(0, "HOST");
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        auto args = parseResult.GetFreeArgs();

        Cerr << "Retries = " << Retries << Endl;
        Cerr << "ResolveTimeout = " << ResolveTimeout.SecondsFloat() << "s" << Endl;
        Cerr << "MaxResolveTimeout = " << MaxResolveTimeout.SecondsFloat() << "s" << Endl;
        Cerr << "WarningTimeout = " << WarningTimeout.SecondsFloat() << "s" << Endl;

        std::optional<double> jitter;
        if (!DisableJitter) {
            jitter = Jitter;
        }
        Cerr << "Jitter = " << ToString(jitter) << Endl;

        auto config = New<TAresDnsResolverConfig>();
        config->Retries = Retries;
        config->ResolveTimeout = ResolveTimeout;
        config->MaxResolveTimeout = MaxResolveTimeout;
        config->WarningTimeout = WarningTimeout;

        auto resolver = CreateAresDnsResolver(config);

        std::vector<TString> hostnames;
        std::vector<TFuture<TNetworkAddress>> futures;

        auto t0 = TInstant::Now();

        auto actionQueue = New<TActionQueue>();

        auto invoker = actionQueue->GetInvoker();

        if (MaxConcurrency != -1) {
            invoker = CreateBoundedConcurrencyInvoker(invoker, MaxConcurrency);
        }

        if (args.empty()) {
            TString hostname;
            while (Cin.ReadLine(hostname)) {
                hostnames.push_back(hostname);
            }
        } else {
            for (const auto& arg : args) {
                hostnames.push_back(arg);
            }
        }

        TDnsResolveOptions options{
            .EnableIPv4 = EnableIPv4,
            .EnableIPv6 = EnableIPv6,
        };
        for (const auto& hostname : hostnames) {
            futures.emplace_back(BIND([&] {
                return WaitFor(resolver->Resolve(hostname, options))
                    .ValueOrThrow();
            })
            .AsyncVia(invoker)
            .Run());
        }

        auto results = AllSet(futures).Get().ValueOrThrow();

        auto t1 = TInstant::Now();

        for (size_t i = 0; i < results.size(); ++i) {
            const auto& hostname = hostnames[i];
            const auto& result = results[i];
            if (result.IsOK()) {
                Cout << "OK\t" << hostname << "\t" << ToString(result.Value());
            } else {
                Cout << "FAIL\t" << hostname << "\t" << result.GetMessage();
            }
            Cout << Endl;
        }

        Cerr << "t = " << (t1 - t0).SecondsFloat() << "s" << Endl;
    }

private:
    int Retries = 25;
    TDuration ResolveTimeout = TDuration::MilliSeconds(500);
    TDuration MaxResolveTimeout = TDuration::MilliSeconds(5000);
    TDuration WarningTimeout = TDuration::MilliSeconds(1000);
    double Jitter = 0.5;
    bool DisableJitter = false;
    int MaxConcurrency = -1;
    bool EnableIPv4;
    bool EnableIPv6;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TDnsTestProgram().Run(argc, argv);
}
