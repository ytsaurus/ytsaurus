#pragma once

#include "id_to_path_updater.h"

NRoren::TPCollection<NRoren::TKV<ui64, TIdToPathRow>> ApplyMapper(NRoren::TPCollection<std::string> input, std::string forceCluster, THashSet<std::string> allowClusters);
