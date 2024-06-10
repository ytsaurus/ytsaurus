#pragma once

#include "pull_client.h"
#include "auth.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace NSolomon {
    const TString SOLOMON_PROD_PUSH_URL = "https://solomon.yandex.net/api/v2/push";
    const TString SOLOMON_PRE_PUSH_URL = "https://solomon-prestable.yandex.net/api/v2/push";

    struct TSolomonPushConfig {
        TSolomonPushConfig(TString endpoint = SOLOMON_PROD_PUSH_URL, IAuthProviderPtr authProvider = nullptr);

        TString Server;
        TString Path;
        ui16 Port = 80;

        TString Project;
        TString Cluster;
        TString Service;
        TString Host;

        ui64 RetryCount = 40;
        TDuration RetrySleep = TDuration::Seconds(3);
        TDuration TimeoutSocket = TDuration::Seconds(5);
        TDuration TimeoutConnection = TDuration::Seconds(30);

        TSolomonPushConfig& WithRetryCount(const ui64 retryCount) {
            RetryCount = retryCount;
            return *this;
        }

        TSolomonPushConfig& WithRetrySleep(const TDuration& retrySleep) {
            RetrySleep = retrySleep;
            return *this;
        }

        TSolomonPushConfig& WithTimeoutSocket(const TDuration& timeoutSocket) {
            TimeoutSocket = timeoutSocket;
            return *this;
        }

        TSolomonPushConfig& WithTimeoutConnection(const TDuration& timeoutConnection) {
            TimeoutConnection = timeoutConnection;
            return *this;
        }

        TSolomonPushConfig& WithProject(const TString& project) {
            Project = project;
            return *this;
        }
        TSolomonPushConfig& WithCluster(const TString& cluster) {
            Cluster = cluster;
            return *this;
        }
        TSolomonPushConfig& WithService(const TString& service) {
            Service = service;
            return *this;
        }
        TSolomonPushConfig& WithHost(const TString& host) {
            Host = host;
            return *this;
        }

        const IAuthProvider& AuthProvider() const;

    private:
        IAuthProviderPtr AuthProvider_;
    };

    class TSolomonPushJsonBuilder: public TSolomonJsonBuilder {
    public:
        TSolomonPushJsonBuilder(const TSolomonPushConfig& config, EFormat format = EFormat::Json);
        void PushToSolomon();

    protected:
        void Finish();

    private:
        const TSolomonPushConfig Config_;
    };

} // namespace NSolomon
