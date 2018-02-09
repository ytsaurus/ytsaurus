#pragma once

#include "public.h"

#include "rb_torrent.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/ypath/public.h>

#include <yt/core/http/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TSkynetManager
    : public TRefCounted
{
public:
    explicit TSkynetManager(TBootstrap* bootstrap);

    void Share(NHttp::IRequestPtr req, NHttp::IResponseWriterPtr rsp);
    void Discover(NHttp::IRequestPtr req, NHttp::IResponseWriterPtr rsp);

    TClusterConnectionConfigPtr GetCluster(const TString& name);

private:
    TBootstrap* const Bootstrap_;
};

DEFINE_REFCOUNTED_TYPE(TSkynetManager);

////////////////////////////////////////////////////////////////////////////////
    
struct THttpPartLocation
{
    TString Filename;
    i64 RangeStart;
    i64 RangeEnd;

    //! HTTP url directly to data nodes.
    std::vector<TString> Locations;
};

void Serialize(const THttpPartLocation& partLocation, NYson::IYsonConsumer* consumer);

struct THttpReply
{
    std::vector<THttpPartLocation> Parts;
};

void Serialize(const THttpReply& reply, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TFileOffset
{
    TString FilePath;
    ui64 StartRow;
    ui64 RowCount = 0;
    ui64 FileSize = 0;
};

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

} // namespace NSkynetManager
} // namespace NYT
