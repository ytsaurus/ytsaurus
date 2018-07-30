#pragma once

#include <Client/ConnectionPoolWithFailover.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>

namespace NYT {
namespace NClickHouse {

struct TClusterNodeName
{
    std::string Host;
    uint64_t Port;

    std::string ToString() const
    {
        return Host + ":" + std::to_string(Port);
    }

    bool operator ==(const TClusterNodeName& that) const
    {
        return std::tie(Host, Port) == std::tie(that.Host, that.Port);
    }

    bool operator !=(const TClusterNodeName& that) const
    {
        return !(*this == that);
    }

    bool operator <(const TClusterNodeName& that) const
    {
        return std::tie(Host, Port) < std::tie(that.Host, that.Port);
    }
};

}   // namespace NClickHouse
}   // namespace NYT

namespace std {

template <>
struct hash<NYT::NClickHouse::TClusterNodeName>
{
    size_t operator()(const NYT::NClickHouse::TClusterNodeName& name) const
    {
        return std::hash<std::string>()(name.Host);
    }
};

}   // namespace std

namespace NYT {
namespace NClickHouse {

using TClusterNodeNames = std::unordered_set<TClusterNodeName>;

////////////////////////////////////////////////////////////////////////////////

class IClusterNode
{
public:
    virtual ~IClusterNode() = default;

    virtual TClusterNodeName GetName() const = 0;
    virtual bool IsLocal() const = 0;
    virtual DB::ConnectionPoolWithFailoverPtr GetConnection() = 0;
};

using IClusterNodePtr = std::shared_ptr<IClusterNode>;

using TClusterNodes = std::vector<IClusterNodePtr>;

////////////////////////////////////////////////////////////////////////////////

IClusterNodePtr CreateClusterNode(
    const TClusterNodeName& name,
    const DB::Settings& settings);

} // namespace NClickHouse
} // namespace NYT
