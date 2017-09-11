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
        TPartIndexList erasedIndices;
        auto allReaders = Readers_;
        std::vector<int> readersParts(Readers_.size());
        std::iota(readersParts.begin(), readersParts.end(), 0);
        for (;;) {
            auto reader = CreateRepairingErasureReader(Codec_, erasedIndices, Readers_);
            auto result = WaitFor(reader->ReadBlocks(WorkloadDescriptor_, BlockIndexes_));
            if (result.IsOK()) {
                return result.Value();
            } else if (!Config_->EnableAutoRepair) {
                THROW_ERROR result;
            }

            for (size_t i = 0; i < Readers_.size(); ++i) {
                if (!Readers_[i]->IsValid()) {
                    erasedIndices.push_back(readersParts[i]);
                }
            }
            std::sort(erasedIndices.begin(), erasedIndices.end());

            auto repairIndicesResult = Codec_->GetRepairIndices(erasedIndices);
            if (!repairIndicesResult) {
                THROW_ERROR (result << TError("Not enough parts to repair"));
            }
            auto repairIndices = repairIndicesResult.Get();

            std::vector<IChunkReaderPtr> newReaders;
            std::vector<int> newReadersParts;
            for (int i = 0; i < Codec_->GetDataPartCount(); ++i) {
                if (!std::binary_search(erasedIndices.begin(), erasedIndices.end(), i)) {
                    newReaders.push_back(allReaders[i]);
                    newReadersParts.push_back(i);
                }
            }
            for (int i = Codec_->GetDataPartCount(); i < Codec_->GetTotalPartCount(); ++i) {
                if (std::binary_search(repairIndices.begin(), repairIndices.end(), i)) {
                    newReaders.push_back(allReaders[i]);
                    newReadersParts.push_back(i);
                }
            }
            Readers_.swap(newReaders);
            readersParts.swap(newReadersParts);
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
