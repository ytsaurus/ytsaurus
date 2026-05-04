#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYql::NFmr {

    ////////////////////////////////////////////////////////////////////////////////

    struct TVanillaPeerTrackerSettings {
        TString Cluster;
        ui64 JobCount = 1;
        TDuration ListJobsInterval = TDuration::Seconds(1);
        TDuration PingClientInterval = TDuration::Seconds(1);
        TDuration PingTimeout = TDuration::Seconds(5);
    };

    class TVanillaPeerTracker {
    public:
        explicit TVanillaPeerTracker(TVanillaPeerTrackerSettings settings);

        TString GetOperationId() const;
        ui64 GetSelfIndex() const;
        TString GetSelfJobId() const;
        TString GetSelfIpAddress() const;
        ui64 GetPeerCount() const;
        // IP address may be empty
        TString GetPeerAddress(ui64 index) const;
        // some IP addresses may be empty
        TVector<TString> GetPeerAddresses() const;

        // this method should be called once inside Do method
        // it returns only if this job should exit
        void Run();

        // Lists jobs in the given operation, prints the job with cookie 0.
        // If withPing is true, also pings that job's IP and reports the result.
        static void CheckOperation(
            const TString& cluster,
            const TString& operationId,
            bool withPing = false,
            TDuration pingTimeout = TDuration::Seconds(5));

    private:
        const TVanillaPeerTrackerSettings Settings_;
        const ui64 SelfCookie_;
        const TString SelfJobId_;
        const TString SelfIpAddress_;
        const TString OperationId_;
        mutable TMutex PeersMutex_;
        TVector<TString> PeerIps_;
        std::atomic<bool> Shutdown_{false};
        THolder<TThread> ServerThread_;
        THolder<TThread> ClientThread_;
    };

    ////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
