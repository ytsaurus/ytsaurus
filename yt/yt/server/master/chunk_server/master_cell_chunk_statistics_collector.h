#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUpdateMasterCellChunkStatistics;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct IMasterCellChunkStatisticsPieceCollector
    : public TRefCounted
{
    virtual void PersistentClear() noexcept = 0;
    virtual void TransientClear() noexcept = 0;

    virtual void OnChunkCreated(TChunk* chunk) noexcept = 0;
    virtual void OnChunkDestroyed(TChunk* chunk) noexcept = 0;

    virtual void OnChunkScan(TChunk* chunk) noexcept = 0;
    virtual bool ScanNeeded() const noexcept = 0;

    //! Returns `false` if there is nothing to update.
    virtual bool FillUpdateRequest(NProto::TReqUpdateMasterCellChunkStatistics& request) const = 0;
    // NB: Update request filling is two-phase in order to handle errors properly.
    virtual void OnAfterUpdateRequestFilled() noexcept = 0;

    virtual void OnScanScheduled(bool isLeader) = 0;
    virtual void OnUpdateStatistics(const NProto::TReqUpdateMasterCellChunkStatistics& request) = 0;

    virtual void Save(NCellMaster::TSaveContext& context) const = 0;
    virtual void Load(NCellMaster::TLoadContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterCellChunkStatisticsPieceCollector)

////////////////////////////////////////////////////////////////////////////////

struct IMasterCellChunkStatisticsCollector
    : public virtual TRefCounted
{
    virtual void OnChunkCreated(TChunk* chunk) = 0;
    virtual void OnChunkDestroyed(TChunk* chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterCellChunkStatisticsCollector)

////////////////////////////////////////////////////////////////////////////////

IMasterCellChunkStatisticsCollectorPtr CreateMasterCellChunkStatisticsCollector(
    NCellMaster::TBootstrap* bootstrap,
    std::vector<IMasterCellChunkStatisticsPieceCollectorPtr> collectors);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
