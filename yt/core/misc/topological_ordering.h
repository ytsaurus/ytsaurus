#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Class representing a directed acyclic graph with its topological ordering
//! allowing you to insert new edges in the graph.
/*!
 * Vertices of graph are represented with integers.
 * TODO(max42): http://people.cs.georgetown.edu/~jfineman/papers/topsort.pdf :)
 */
class TIncrementalTopologicalOrdering
{
public:
    TIncrementalTopologicalOrdering() = default;

    const std::vector<int>& GetOrdering() const;

    void AddEdge(int from, int to);

    void Persist(const TStreamPersistenceContext& context);

private:
    std::vector<int> TopologicalOrdering_;
    THashMap<int, THashSet<int>> OutgoingEdges_;

    void Rebuild();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT