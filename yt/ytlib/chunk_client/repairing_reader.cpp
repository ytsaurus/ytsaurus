#include "config.h"
#include "repairing_reader.h"
#include "erasure_repair.h"
#include "erasure_helpers.h"
#include "dispatcher.h"

#include <yt/ytlib/misc/workload.h>

#include <yt/core/concurrency/scheduler.h>

#include <algorithm>
#include <vector>

namespace NYT {
namespace NChunkClient {

using namespace NErasure;
using namespace NConcurrency;
using namespace NChunkClient::NProto;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

class TRepairingReaderSession
    : public TRefCounted
{
public:
    TRepairingReaderSession(
        ICodec* codec,
        const TErasureReaderConfigPtr config,
        const std::vector<IChunkReaderPtr>& readers,
        const TErasurePlacementExt& placementExt,
        const std::vector<int>& blockIndexes,
        const TWorkloadDescriptor& workloadDescriptor)
        : Codec_(codec)
        , Config_(config)
        , Readers_(readers)
        , PlacementExt_(placementExt)
        , BlockIndexes_(blockIndexes)
        , WorkloadDescriptor_(workloadDescriptor)
        , DataBlocksPlacementInParts_(BuildDataBlocksPlacementInParts(BlockIndexes_, PlacementExt_))
    {
        if (Config_->EnableAutoRepair) {
            YCHECK(Readers_.size() == Codec_->GetTotalPartCount());
        } else {
            YCHECK(Readers_.size() == Codec_->GetDataPartCount());
        }
    }

    TFuture<std::vector<TBlock>> Run()
    {
        return BIND(&TRepairingReaderSession::BuildResult, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

private:
    ICodec* const Codec_;
    const TErasureReaderConfigPtr Config_;
    std::vector<IChunkReaderPtr> Readers_;
    const TErasurePlacementExt PlacementExt_;
    const std::vector<int> BlockIndexes_;
    const TWorkloadDescriptor WorkloadDescriptor_;
    TDataBlocksPlacementInParts DataBlocksPlacementInParts_;

    std::vector<TBlock> BuildResult()
    {
        if (!Config_->EnableAutoRepair) {
            auto reader = CreateRepairingErasureReader(Codec_, TPartIndexList(), Readers_);
            return WaitFor(reader->ReadBlocks(WorkloadDescriptor_, BlockIndexes_))
                .ValueOrThrow();
        }

        TNullable<TPartIndexList> erasedIndicesOnPreviousIteration;
        TError error;

        while (true) {
            TPartIndexList erasedIndices;
            for (size_t index = 0; index < Readers_.size(); ++index) {
                if (!Readers_[index]->IsValid()) {
                    erasedIndices.push_back(index);
                }
            }

            if (erasedIndicesOnPreviousIteration && erasedIndices == *erasedIndicesOnPreviousIteration) {
                THROW_ERROR_EXCEPTION("Read with reapir failed, but list of valid underlying part readers do not changed")
                    << error;
            }

            auto repairIndicesOrNull = Codec_->GetRepairIndices(erasedIndices);
            if (!repairIndicesOrNull) {
                THROW_ERROR_EXCEPTION("Not enough parts to read with repair");
            }
            auto repairIndices = *repairIndicesOrNull;

            std::vector<IChunkReaderPtr> readers;
            for (int index = 0; index < Codec_->GetDataPartCount(); ++index) {
                if (!std::binary_search(erasedIndices.begin(), erasedIndices.end(), index)) {
                    readers.push_back(Readers_[index]);
                }
            }
            for (int index = Codec_->GetDataPartCount(); index < Codec_->GetTotalPartCount(); ++index) {
                if (std::binary_search(repairIndices.begin(), repairIndices.end(), index)) {
                    readers.push_back(Readers_[index]);
                }
            }

            auto reader = CreateRepairingErasureReader(Codec_, erasedIndices, readers);
            auto result = WaitFor(reader->ReadBlocks(WorkloadDescriptor_, BlockIndexes_));

            if (result.IsOK()) {
                return result.Value();
            } else {
                error = result;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRepairingReader
    : public TErasureChunkReaderBase
{
public:
    TRepairingReader(
        ICodec* codec,
        TErasureReaderConfigPtr config,
        const std::vector<IChunkReaderPtr>& readers)
        : TErasureChunkReaderBase(codec, readers)
        , Config_(config)
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<int>& blockIndexes) override
    {
        return PreparePlacementMeta(workloadDescriptor).Apply(
            BIND([=, this_ = MakeStrong(this)] () {
                auto session = New<TRepairingReaderSession>(
                    Codec_,
                    Config_,
                    Readers_,
                    PlacementExt_,
                    blockIndexes,
                    workloadDescriptor);
                return session->Run();
            }));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        int firstBlockIndex,
        int blockCount) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual bool IsValid() const override
    {
        return true;
    }

private:
    const TErasureReaderConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRepairingReader(
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderPtr>& readers)
{
    return New<TRepairingReader>(
        codec,
        config,
        readers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
