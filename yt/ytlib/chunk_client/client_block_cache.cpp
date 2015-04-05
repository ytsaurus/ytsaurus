#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "block_cache.h"
#include "client_block_cache.h"

#include <core/misc/sync_cache.h>
#include <core/misc/property.h>
#include <core/misc/singleton.h>

#include <ytlib/chunk_client/block_id.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NChunkClient {

using namespace NNodeTrackerClient;

///////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

class TCachedBlock
    : public TSyncCacheValueBase<TBlockId, TCachedBlock>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TSharedRef, Data);

public:
    TCachedBlock(const TBlockId& id, const TSharedRef& data)
        : TSyncCacheValueBase(id)
        , Data_(data)
    { }

};

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPerTypeClientBlockCache)

class TPerTypeClientBlockCache
    : public TSyncSlruCacheBase<TBlockId, TCachedBlock>
{
public:
    TPerTypeClientBlockCache(
        EBlockType type,
        TSlruCacheConfigPtr config,
        const NProfiling::TProfiler& profiler)
        : TSyncSlruCacheBase(config, profiler)
        , Type_(type)
    { }

    void Put(const TBlockId& id, const TSharedRef& data)
    {
        auto block = New<TCachedBlock>(id, data);
        if (TryInsert(block)) {
            LOG_DEBUG("Block is put into cache (BlockId: %v, BlockType: %v, BlockSize: %v)",
                id,
                Type_,
                data.Size());
        } else {
            // Already have the block cached, do nothing.
            LOG_TRACE("Block is already in cache (BlockId: %v, BlockType: %v)",
                id,
                Type_);
        }
    }

    TSharedRef Find(const TBlockId& id)
    {
        auto block = TSyncSlruCacheBase::Find(id);
        if (block) {
            LOG_TRACE("Block cache hit (BlockId: %v, BlockType: %v)",
                id,
                Type_);
            return block->GetData();
        } else {
            LOG_TRACE("Block cache miss (BlockId: %v, BlockType: %v)",
                id,
                Type_);
            return TSharedRef();
        }
    }

private:
    const EBlockType Type_;

    virtual i64 GetWeight(TCachedBlock* block) const override
    {
        return block->GetData().Size();
    }

};

DEFINE_REFCOUNTED_TYPE(TPerTypeClientBlockCache)

///////////////////////////////////////////////////////////////////////////////

class TClientBlockCache
    : public IBlockCache
{
public:
    TClientBlockCache(
        TBlockCacheConfigPtr config,
        const std::vector<EBlockType>& blockTypes,
        const NProfiling::TProfiler& profiler)
    {
        auto initType = [&] (EBlockType type, TSlruCacheConfigPtr config) {
            if (std::find(blockTypes.begin(), blockTypes.end(), type) != blockTypes.end()) {
                PerTypeCaches_[type] = New<TPerTypeClientBlockCache>(
                    type,
                    config,
                    NProfiling::TProfiler(profiler.GetPathPrefix() + "/" + FormatEnum(type)));
            }
        };
        initType(EBlockType::CompressedData, config->CompressedData);
        initType(EBlockType::UncompressedData, config->UncompressedData);
    }

    virtual void Put(
        const TBlockId& id,
        EBlockType type,
        const TSharedRef& data,
        const TNullable<TNodeDescriptor>& /*source*/) override
    {
        const auto& cache = PerTypeCaches_[type];
        if (cache) {
            cache->Put(id, data);
        }
    }

    virtual TSharedRef Find(
        const TBlockId& id,
        EBlockType type) override
    {
        const auto& cache = PerTypeCaches_[type];
        return cache ? PerTypeCaches_[type]->Find(id) : TSharedRef();
    }

private:
    TEnumIndexedVector<TPerTypeClientBlockCachePtr, EBlockType> PerTypeCaches_;

};

IBlockCachePtr CreateClientBlockCache(
    TBlockCacheConfigPtr config,
    const std::vector<EBlockType>& blockTypes,
    const NProfiling::TProfiler& profiler)
{
    return New<TClientBlockCache>(config, blockTypes, profiler);
}

///////////////////////////////////////////////////////////////////////////////

class TNullBlockCache
    : public IBlockCache
{
public:
    virtual void Put(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TSharedRef& /*data*/,
        const TNullable<TNodeDescriptor>& /*source*/) override
    { }

    virtual TSharedRef Find(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return TSharedRef();
    }
};

IBlockCachePtr GetNullBlockCache()
{
    return RefCountedSingleton<TNullBlockCache>();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

