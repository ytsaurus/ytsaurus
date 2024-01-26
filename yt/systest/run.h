#pragma once

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/systest/operation.h>
#include <yt/systest/table.h>

namespace NYT::NTest {

void RunMap(IClientPtr client, const TString& pool,
            const TString& inputPath, const TString& outputPath,
            const TTable& inputTable, const TTable& outputTable, const IMultiMapper& operation);

void RunReduce(IClientPtr client, const TString& pool,
               const TString& inputPath, const TString& outputPath,
               const TTable& table, const TTable& outputTable, const TReduceOperation& operation);

void RunSort(IClientPtr client, const TString& pool,
             const TString& inputPath, const TString& outputPath,
             const TSortColumns& sortColumns);

}  // namespace NYT::NTest
