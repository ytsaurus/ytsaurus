#pragma once

#include "public.h"

#include "rb_torrent.h"
#include "share_cache.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/ypath/public.h>

#include <yt/core/http/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TSkynetManager
    : public IShareHost
{
public:
    explicit TSkynetManager(TBootstrap* bootstrap);

    void HealthCheck(NHttp::IRequestPtr req, NHttp::IResponseWriterPtr rsp);

    void Share(NHttp::IRequestPtr req, NHttp::IResponseWriterPtr rsp);
    void Discover(NHttp::IRequestPtr req, NHttp::IResponseWriterPtr rsp);

    void RunCypressSyncIteration(const TCluster& cluster);
    void RunTableScanIteration(const TCluster& cluster);

    TString FormatDiscoveryUrl(const TString& rbTorrentId);
    TErrorOr<i64> CheckTableAttributes(
        const TString& clusterName,
        const NYPath::TRichYPath& path);

    //! Overrides from IShareHost
    virtual IInvokerPtr GetInvoker() override;

    virtual std::pair<TSkynetShareMeta, std::vector<TFileOffset>> ReadMeta(
        const TString& clusterName,
        const NYPath::TRichYPath& path) override;

    virtual void AddShareToCypress(
        const TString& clusterName,
        const TString& rbTorrentId,
        const NYPath::TRichYPath& tablePath,
        i64 tableRevision) override;
    virtual void RemoveShareFromCypress(
        const TString& clusterName,
        const TString& rbTorrentId) override;

    virtual void AddResourceToSkynet(
        const TString& rbTorrentId,
        const TString& rbTorrent) override;
    virtual void RemoveResourceFromSkynet(
        const TString& rbTorrentId) override;

private:
    const TBootstrap* Bootstrap_;

    TSpinLock Lock_;
    THashSet<TString> SyncedClusters_;
    THashMap<TString, TError> CypressPullErrors_;
    THashMap<TString, TError> TablesScanErrors_;
};

DEFINE_REFCOUNTED_TYPE(TSkynetManager);

////////////////////////////////////////////////////////////////////////////////
    
struct THttpPartLocation
{
    TString Filename;
    i64 RangeStart;
    i64 RangeEnd;

    //! HTTP URL directly to data nodes.
    std::vector<TString> Locations;
};

void Serialize(const THttpPartLocation& partLocation, NYson::IYsonConsumer* consumer);

struct THttpReply
{
    std::vector<THttpPartLocation> Parts;
};

void Serialize(const THttpReply& reply, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

std::pair<TSkynetShareMeta, std::vector<TFileOffset>> ReadSkynetMetaFromTable(
    const NHttp::IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const NYPath::TRichYPath& path);

std::vector<THttpPartLocation> FetchSkynetPartsLocations(
    const NHttp::IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const NYPath::TRichYPath& path,
    const std::vector<TFileOffset>& offsets);

////////////////////////////////////////////////////////////////////////////////

void CleanSkynet(const ISkynetApiPtr& skynet, const TShareCachePtr& shareCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
