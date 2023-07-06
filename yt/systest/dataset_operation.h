#pragma once

#include <yt/systest/dataset.h>
#include <yt/systest/operation.h>

namespace NYT::NTest {

std::unique_ptr<IMultiMapper> CreateRandomMap(
    std::mt19937& randomEngine, const TStoredDataset& info);

}  // namespace NYT::NTest
