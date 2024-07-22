#pragma once

#include "yt_graph_v2.h"

#include <yt/cpp/roren/interface/private/par_do_tree.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TMergeOutputConnector
{
    bool operator==(const TMergeOutputConnector& other) const = default;
};

struct TSortOutputConnector
{
    bool operator==(const TSortOutputConnector& other) const = default;
};

struct TStatefulConnector
{
    bool operator==(const TStatefulConnector& other) const = default;
};

struct TMapperOutputConnector
{
    // Index of a mapper inside operation
    ssize_t MapperIndex = 0;
    TParDoTreeBuilder::TPCollectionNodeId NodeId = TParDoTreeBuilder::RootNodeId;

    bool operator==(const TMapperOutputConnector& other) const = default;
};

struct TReducerOutputConnector
{
    TParDoTreeBuilder::TPCollectionNodeId NodeId = TParDoTreeBuilder::RootNodeId;

    bool operator==(const TReducerOutputConnector& other) const = default;
};

using TOperationConnector = std::variant<TMergeOutputConnector, TSortOutputConnector, TStatefulConnector, TMapperOutputConnector, TReducerOutputConnector>;

////////////////////////////////////////////////////////////////////////////////

TParDoTreeBuilder::TPCollectionNodeId VerifiedGetNodeIdOfMapperConnector(const TOperationConnector& connector);

////////////////////////////////////////////////////////////////////////////////

template <class TConnector>
TString ConnectorToString(const TConnector& connector) {
    return std::visit([] (const auto& connector) {
        using TType = std::decay_t<decltype(connector)>;
        if constexpr (std::is_same_v<TMergeOutputConnector, TType>) {
            return ToString("Merge");
        } else if constexpr (std::is_same_v<TSortOutputConnector, TType>) {
            return ToString("Sort");
        } else if constexpr (std::is_same_v<TStatefulConnector, TType>) {
            return ToString("Stateful");
        } else if constexpr (std::is_same_v<TMapperOutputConnector, TType>) {
            return ToString("Map.") + ToString(connector.MapperIndex) + "." + ToString(connector.NodeId);
        } else if constexpr (std::is_same_v<TReducerOutputConnector, TType>) {
            return ToString("Reduce.") + ToString(connector.NodeId);
        } else {
            static_assert(TDependentFalse<TType>);
        }
    }, connector);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

template <>
struct THash<NRoren::NPrivate::TOperationConnector>
{
    size_t operator() (const NRoren::NPrivate::TOperationConnector& connector) {
        using namespace NRoren::NPrivate;
        return std::visit([] (const auto& connector) -> size_t {
            using TType = std::decay_t<decltype(connector)>;
            if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                // Carefuly and randomly generated number.
                return 1933246098;
            } else if constexpr (std::is_same_v<TType, TSortOutputConnector>) {
                // Carefuly and randomly generated number.
                return 400756313;
            } else if constexpr (std::is_same_v<TType, TStatefulConnector>) {
                // Carefuly and randomly generated number.
                return 13376942010;
            } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>) {
                auto val = std::tuple{0, connector.MapperIndex, connector.NodeId};
                return THash<decltype(val)>()(val);
            } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                auto val = std::tuple{1, connector.NodeId};
                return THash<decltype(val)>()(val);
            } else {
                static_assert(TDependentFalse<TType>);
            }
        }, connector);
    }
};
