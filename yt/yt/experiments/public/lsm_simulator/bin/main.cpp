#include <yt/yt/experiments/public/lsm_simulator/lib/action_queue.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/helpers.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/simulator.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/store_manager.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/structured_logger.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/tablet.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/tabular_formatter.h>
#include <yt/yt/experiments/public/lsm_simulator/lib/mount_config_optimizer.h>

#include <yt/yt/server/lib/lsm/tablet.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/moving_average.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/stream/file.h>

#include <library/cpp/getopt/last_getopt.h>

using namespace NYT::NLsm::NTesting;
using namespace NYT;
using namespace NYT::NTabletNode;

TString FormatDataSize(i64 value) {
    for (auto suffix : {"B", "KB", "MB", "GB", "TB"}) {
        if (value < 1000) {
            return ToString(value) + " " + suffix;
        } else if (value < 1024) {
            value = 1024;
        }
        value /= 1024;
    }
    return "inf";
}

TTabletPtr LoadTablet(TString filename, TTableMountConfigPtr mountConfig)
{
    TFile file(filename, EOpenModeFlag::RdOnly);
    TUnbufferedFileInput stream(file);
    TStreamLoadContext context(&stream);
    auto tablet = New<TTablet>();
    NYT::Load(context, *tablet);
    if (file.GetPosition() < file.GetLength()) {
        NYT::Load(context, EpochIndex);
        ++EpochIndex;
    } else {
        EpochIndex = 2;
    }
    tablet->SetMountConfig(std::move(mountConfig));
    return tablet;
}

void OptimizeWithMountConfigOptimizer(
    std::optional<TString> loadFromFile,
    const TTableMountConfigPtr& mountConfig,
    const TLsmSimulatorConfigPtr& simulatorConfig)
{
    TMountConfigOptimizer optimizer(
        simulatorConfig->Optimizer,
        mountConfig);

    auto doEval = [&] (TTableMountConfigPtr mountConfig) -> double {
        TTabletPtr tablet;

        if (loadFromFile) {
            tablet = LoadTablet(*loadFromFile, mountConfig);
        }

        auto actionQueue = CreateActionQueue(TInstant::Now());
        auto simulator = CreateLsmSimulator(
            simulatorConfig,
            mountConfig,
            actionQueue,
            tablet);
        simulator->Start();

        while (!simulator->IsWritingCompleted()) {
            actionQueue->RunFor(TDuration::Seconds(10));
        }
        actionQueue->RunFor(TDuration::Seconds(30));

        auto stats = simulator->GetStoreManager()->GetStatistics();
        return stats.BytesCompacted + stats.BytesPartitioned;
    };

    auto optimalMountConfig = optimizer.Run(doEval);

    Cerr << ConvertToYsonString(optimalMountConfig, NYT::NYson::EYsonFormat::Pretty).AsStringBuf() << Endl;
}

int main(int argc, char* argv[])
{
    NLastGetopt::TOpts opts;
    TString saveToFile;
    TString loadFromFile;
    opts.AddLongOption("save")
        .RequiredArgument("filename")
        .StoreResult(&saveToFile);
    opts.AddLongOption("load")
        .RequiredArgument("filename")
        .StoreResult(&loadFromFile);
    opts.AddLongOption("optimize")
        .NoArgument();
    NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);

    auto now = TInstant::Now();
    auto actionQueue = CreateActionQueue(now);
    auto mountConfig = ReadYsonSerializableWithComments<TTableMountConfigPtr>("mount_config.yson");
    auto simulatorConfig = ReadYsonSerializableWithComments<TLsmSimulatorConfigPtr>("config.yson");

    if (parsed.Has("optimize")) {
        OptimizeWithMountConfigOptimizer(
            parsed.Has("load") ? std::make_optional(loadFromFile) : std::nullopt,
            mountConfig,
            simulatorConfig);
        return 0;
    }

    TStructuredLoggerPtr structuredLogger = simulatorConfig->EnableStructureLogging
        ? New<TStructuredLogger>()
        : nullptr;

    TTabletPtr tablet;

    if (parsed.Has("load")) {
        tablet = LoadTablet(loadFromFile, mountConfig);
        tablet->SetStructuredLogger(structuredLogger);
    }

    auto simulator = CreateLsmSimulator(simulatorConfig, mountConfig, actionQueue, tablet, structuredLogger);
    simulator->Start();

    TMovingAverage<NLsm::NTesting::TStatistics> statisticsAvg(simulatorConfig->StatisticsWindowSize);
    NLsm::NTesting::TStatistics currentStatistics{};
    NLsm::NTesting::TStatistics pivotStatistics{};
    TMovingAverage<int> oscAvg(simulatorConfig->StatisticsWindowSize);
    double sumCompactionWA = 0;
    double maxCompactionWA = 0;
    double sumPartitioningWA = 0;
    double maxPartitioningWA = 0;
    double sumTotalWA = 0;
    double maxTotalWA = 0;
    int sumOSC = 0;
    int maxOSC = 0;
    int periodСounter = 0;
    int writerIndex = 0;

    TTabularFormatter formatter(ssize(simulatorConfig->LoggingFields));

    while (!actionQueue->IsStopped()) {
        actionQueue->RunFor(simulatorConfig->LoggingPeriod);
        auto* storeManager = simulator->GetStoreManager();
        oscAvg.AddValue(storeManager->GetTablet()->GetOverlappingStoreCount());

        {
            auto nextStatistics = storeManager->GetStatistics();
            if (simulatorConfig->SumCompactionAndPartitioningInLogging) {
                nextStatistics.BytesCompacted += nextStatistics.BytesPartitioned;
                nextStatistics.BytesPartitioned = 0;
            }
            statisticsAvg.AddValue(nextStatistics - currentStatistics);
            currentStatistics = nextStatistics;
        }

        auto effectiveStatistics = currentStatistics - pivotStatistics;

        double compactionWA = effectiveStatistics.BytesWritten != 0
            ? 1.0 * effectiveStatistics.BytesCompacted / effectiveStatistics.BytesWritten
            : 0;
        double partitioningWA = effectiveStatistics.BytesWritten != 0
            ? 1.0 * effectiveStatistics.BytesPartitioned / effectiveStatistics.BytesWritten
            : 0;
        double totalWA = compactionWA + partitioningWA;
        sumCompactionWA += compactionWA;
        sumPartitioningWA += partitioningWA;
        sumTotalWA += totalWA;
        maxCompactionWA = std::max(maxCompactionWA, compactionWA);
        maxPartitioningWA = std::max(maxPartitioningWA, partitioningWA);
        maxTotalWA = std::max(maxTotalWA, totalWA);
        int currentOSC = storeManager->GetTablet()->GetOverlappingStoreCount();
        sumOSC += currentOSC;
        maxOSC = std::max(maxOSC, currentOSC);
        ++periodСounter;

        auto timePassed = actionQueue->GetNow() - now;
        i64 secondsPassed = timePassed.Seconds();

        std::vector<TString> logLine;
        for (const auto& field : simulatorConfig->LoggingFields) {
            TString str;
            auto on = [&] (TStringBuf title, i64 value, bool rate = false) {
                str += title;
                str += " " + FormatDataSize(value) + (rate ? "/s" : "");
            };

            int secsPerTick = simulatorConfig->LoggingPeriod.Seconds();

            auto statsAvg = statisticsAvg.GetAverage().value_or(NLsm::NTesting::TStatistics{});
            if (field == "inserted") {
                on("Ins", effectiveStatistics.BytesWritten);
            } else if (field == "inserting") {
                on("Ins", statsAvg.BytesWritten / secsPerTick, true);
            } else if (field == "size") {
                on("Size", storeManager->GetCompressedDataSize());
            } else if (field == "compacted") {
                on("Comp", effectiveStatistics.BytesCompacted);
            } else if (field == "compacting") {
                on("Comp", statsAvg.BytesCompacted / secsPerTick, true);
            } else if (field == "compacted_avg") {
                on("Comp avg", effectiveStatistics.BytesCompacted / std::max<i64>(1, secondsPassed), true);
            } else if (field == "partitioned") {
                on("Part", effectiveStatistics.BytesPartitioned);
            } else if (field == "partitioning") {
                on("Part", statsAvg.BytesPartitioned / secsPerTick, true);
            } else if (field == "partitioned_avg") {
                on("Part avg", effectiveStatistics.BytesPartitioned / std::max<i64>(1, secondsPassed), true);
            } else if (field == "write_amplification_compaction") {
                str = Format("Comp WA %.3f", compactionWA);
            } else if (field == "write_amplification_partitioning") {
                str = Format("Part WA %.3f", partitioningWA);
            } else if (field == "write_amplification") {
                str = Format("WA %.3f", totalWA);
            } else if (field == "osc") {
                str = "OSC " + ToString(oscAvg.GetAverage().value_or(0));
            } else if (field == "partition_count") {
                str = "#Part " + ToString(storeManager->GetTablet()->Partitions().size());
            } else if (field == "dynamic_memory_usage") {
                on("DynMem", storeManager->GetDynamicMemoryUsage());
            } else if (field == "time") {
                i64 x = secondsPassed;
                str = ToString(x % 60) + "s";
                x /= 60;
                if (x > 0) {
                    str = ToString(x % 60) + "m " + str;
                    x /= 60;
                    if (x > 0) {
                        str = ToString(x) + "h " + str;
                    }
                }
            } else {
                YT_ABORT();
            }

            logLine.push_back(str);
        }

        Cerr << formatter.Format(logLine) << "\n";

        if (simulatorConfig->EnableStructureLogging) {
            for (const auto& event : structuredLogger->Consume()) {
                Cerr << ToString(event) << "\n";
            }
        }

        if (auto nextWriter = simulator->GetWriterIndex(); nextWriter != writerIndex) {
            if (simulatorConfig->ResetStatisticsAfterLoggers.contains(writerIndex)) {
                pivotStatistics = currentStatistics;
                statisticsAvg.Reset();
                oscAvg.Reset();
                sumTotalWA = 0;
                sumCompactionWA = 0;
                sumPartitioningWA = 0;
                sumOSC = 0;
                periodСounter = 0;
            }

            writerIndex = nextWriter;
        }

        /*
        Cerr << "Inserted " << previousStatistics.BytesWritten / (1 << 30) << " GB  \t" <<
            "Inserting " << statisticsAvg.GetAverage().BytesWritten / (1 << 20) << " MB/s  \t" <<
            "Size " << storeManager->GetCompressedDataSize() / (1 << 20) << " MB  \t" <<
            "Compacting " << statisticsAvg.GetAverage().BytesCompacted / (1 << 20) << " MB/s  \t" <<
            "Partitioning " << statisticsAvg.GetAverage().BytesPartitioned / (1 << 20) << " MB/s  \t" <<
            "OSC " << oscAvg.GetAverage() << " \t" <<
            "Part. count " << storeManager->GetTablet()->Partitions().size() << " \t" <<
            "Time: " << (actionQueue->GetNow() - now) << "\n";
            */

        // storeManager->PrintDebug();

        if (simulator->IsWritingCompleted()) {
            break;
        }
    }

    TTabularFormatter formatterFinal(ssize(simulatorConfig->LoggingFieldsFinal));
    std::vector<TString> logLine;
    for (const auto& field : simulatorConfig->LoggingFieldsFinal) {
        TString str;
        auto statistics = simulator->GetStoreManager()->GetStatistics();

        if (field == "write_amplification_compaction_avg") {
            str = Format("compWAavg %.3f", sumCompactionWA / periodСounter);
        } else if (field == "write_amplification_compaction_max") {
            str = Format("compWAmax %.3f", maxCompactionWA);
        } else if (field == "write_amplification_partitioning_avg") {
            str = Format("partWAavg %.3f", sumPartitioningWA / periodСounter);
        } else if (field == "write_amplification_partitioning_max") {
            str = Format("partWAmax %.3f", maxPartitioningWA);
        } else if (field == "write_amplification_avg") {
            str = Format("WAavg %.3f", sumTotalWA / periodСounter);
        } else if (field == "write_amplification_max") {
            str = Format("WAmax %.3f", maxTotalWA);
        } else if (field == "osc_avg") {
            str = Format("OSCavg %.3f", 1.0 * sumOSC / periodСounter);
        } else if (field == "osc_max") {
            str = "OSCmax " + ToString(maxOSC);
        } else if (field == "total_inserted") {
            str = "Inserted " + ToString(statistics.BytesFlushed);
        } else if (field == "flush_avg") {
            str = "FlushAvg " + ToString(1.0 * statistics.BytesFlushed / statistics.ChunksFlushed);
        } else if (field == "tablet_size") {
            double size = simulator->GetStoreManager()->GetTablet()->Eden()->GetCompressedDataSize();
            for (const auto& partition : simulator->GetStoreManager()->GetTablet()->Partitions()) {
                size += partition->GetCompressedDataSize();
            }
            str = "TabletSize " + ToString(size);
        } else {
            YT_ABORT();
        }

        logLine.push_back(str);
    }

    Cerr << formatterFinal.Format(logLine) << "\n";

    if (parsed.Has("save")) {
        TOFStream stream(saveToFile);
        TStreamSaveContext context(&stream);

        NYT::Save(context, *simulator->GetStoreManager()->GetTablet());
        NYT::Save(context, EpochIndex);
        stream.Finish();
    }

    NYT::Shutdown();
}
