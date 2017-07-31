#pragma once

#include <llvm/ADT/FoldingSet.h>

////////////////////////////////////////////////////////////////////////////////

// A hasher for llvm::FoldingSetNodeID
template <>
struct hash<llvm::FoldingSetNodeID>
{
    inline size_t operator()(const llvm::FoldingSetNodeID& id) const
    {
        return id.ComputeHash();
    }
};

////////////////////////////////////////////////////////////////////////////////