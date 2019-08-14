#pragma once

#include <util/generic/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GetJobInputTypeHashes();
TVector<ui64> GetJobOutputTypeHashes();

void ValidateYdlTypeHash(
    ui64 hash,
    size_t tableIndex,
    const TVector<ui64>& typeHashes,
    bool isRead);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
