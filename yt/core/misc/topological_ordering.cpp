#include "topological_ordering.h"

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const std::vector<int>& TIncrementalTopologicalOrdering::GetOrdering() const
{
    return TopologicalOrdering_;
}

void TIncrementalTopologicalOrdering::AddEdge(int from, int to)
{
    if (OutgoingEdges_[from].insert(to).second) {
        Rebuild();
    }
}

void TIncrementalTopologicalOrdering::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, OutgoingEdges_);
    Persist(context, TopologicalOrdering_);
}

void TIncrementalTopologicalOrdering::Rebuild()
{
    std::queue<int> queue;
    THashMap<int, int> inDegree;

    // Initialize in-degrees of all vertices.
    for (const auto& pair : OutgoingEdges_) {
        int vertex = pair.first;
        // Make an entry for the vertex appear in the inDegree.
        inDegree[vertex];
        const auto& vertexList = pair.second;
        for (int nextVertex : vertexList) {
            ++inDegree[nextVertex];
        }
    }

    // Put all sources in the queue.
    for (const auto& pair : inDegree) {
        int vertex = pair.first;
        int inDegree = pair.second;
        if (inDegree == 0) {
            queue.push(vertex);
        }
    }

    TopologicalOrdering_.clear();

    // Extract sources and put them into the ordering while graph is non-empty.
    while (!queue.empty()) {
        int vertex = queue.front();
        queue.pop();

        auto it = inDegree.find(vertex);
        YCHECK(it != inDegree.end() && it->second == 0);
        inDegree.erase(it);

        TopologicalOrdering_.push_back(vertex);

        for (int nextVertex : OutgoingEdges_[vertex]) {
            if (--inDegree[nextVertex] == 0) {
                queue.push(nextVertex);
            }
        }
    }

    // Check that all vertices are visited.
    YCHECK(inDegree.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT