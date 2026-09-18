#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/error.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NYtflow {

struct TMoniumConnectionConfig {
    TString Endpoint;
    TString Token;
    TDuration Timeout;
    bool EnableSsl = false;

    bool operator==(const TMoniumConnectionConfig& other) const = default;
};

struct TMoniumProject {
    TString Id;
};

struct TMoniumCluster {
    TString Id;
    TString Name;
    TMaybe<TDuration> MetricsTtl;
};

struct TMoniumService {
    TString Id;
    TString Name;
    TMaybe<TDuration> MetricCollectionInterval;
    TMaybe<TString> MetricNameLabel;
    TMaybe<TDuration> Grid;
    TMaybe<TDuration> MetricsTtl;
};

struct TMoniumShard {
    TString Id;
    TString ClusterId;
    TString ServiceId;
    TString ClusterName;
    TString ServiceName;
    TMaybe<TString> MetricNameLabel;
    TMaybe<TDuration> MetricsTtl;
};

struct TListMoniumShardsResult {
    TVector<TMoniumShard> Shards;
    ui64 CurrentPage;
    ui64 PageCount;
};

enum class EMoniumPermission {
    MONIUM_METRICS_WRITE /* "monium.metrics.write" */,
};

DECLARE_REFCOUNTED_CLASS(IMoniumClient);

class IMoniumClient
    : public NYT::TRefCounted
{
public:
    virtual NYT::TFuture<bool> IsPermissionAvailable(
        const TString& project,
        EMoniumPermission permission) const = 0;

    virtual NYT::TFuture<TMoniumProject> GetProject(
        const TString& project) const = 0;

    virtual NYT::TFuture<TMoniumCluster> GetCluster(
        const TString& project,
        const TString& cluster) const = 0;

    virtual NYT::TFuture<TMoniumService> GetService(
        const TString& project,
        const TString& service) const = 0;

    virtual NYT::TFuture<void> CreateCluster(
        const TString& project,
        const TMoniumCluster& cluster) const = 0;

    virtual NYT::TFuture<void> CreateService(
        const TString& project,
        const TMoniumService& service) const = 0;

    virtual NYT::TFuture<TListMoniumShardsResult> ListShards(
        const TString& project,
        i64 page,
        i64 pageSize) const = 0;

    virtual NYT::TFuture<void> CreateShard(
        const TString& project,
        const TMoniumShard& shard) const = 0;
};

IMoniumClientPtr CreateMoniumClient(const TMoniumConnectionConfig& config);

} // namespace NYql::NYtflow

template <>
struct THash<NYql::NYtflow::TMoniumConnectionConfig>
{
    size_t operator()(const NYql::NYtflow::TMoniumConnectionConfig& config) const
    {
        return MultiHash(config.Endpoint, config.Token, config.Timeout, config.EnableSsl);
    }
};
