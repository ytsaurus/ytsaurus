#pragma once

#include <algorithm>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NYql {

class TNodeIdAllocator {
public:
    TNodeIdAllocator(ui32 minNodeId, ui32 maxNodeId)
        : MinNodeId(minNodeId)
        , MaxNodeId(maxNodeId)
    { }

    TVector<ui32> Allocate(const TVector<ui32>& res) {
        TVector<ui32> duplicates;
        for (ui32 id : res) {
            if (!Allocated.insert(id).second) {
                duplicates.push_back(id);
            }
        }
        return duplicates;
    }

    // Atomically allocates all IDs or none. Returns false on any conflict.
    bool TryAllocate(const TVector<ui32>& res) {
        const bool hasConflict = std::any_of(res.begin(), res.end(), [this](ui32 id) {
            return Allocated.contains(id);
        });
        if (hasConflict) {
            return false;
        }
        Allocated.insert(res.begin(), res.end());
        return true;
    }

    void Allocate(TVector<ui32>& res, ui32 count) {
        res.reserve(count);
        for (ui32 id = MinNodeId; id < MaxNodeId && res.size() < count; ++id) {
            if (Allocated.emplace(id).second) {
                res.push_back(id);
            }
        }
    }

    void Deallocate(const TVector<ui32>& nodes) {
        for (auto id : nodes) {
            Allocated.erase(id);
        }
    }

    void Clear() {
        Allocated.clear();
    }

    bool IsAllocated(ui32 id) const {
        return Allocated.contains(id);
    }

private:
    THashSet<ui32> Allocated;
    const ui32 MinNodeId;
    const ui32 MaxNodeId;
};

} // namespace NYql
