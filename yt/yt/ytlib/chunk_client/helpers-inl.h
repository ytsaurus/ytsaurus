#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/core/misc/checksum.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<TUserObject*> MakeUserObjectList(std::vector<T>& vector)
{
    std::vector<TUserObject*> result;
    result.reserve(vector.size());
    for (auto& element : vector) {
        result.push_back(&element);
    }
    return result;
}

template <class T>
std::vector<TUserObject*> MakeUserObjectList(std::vector<TIntrusivePtr<T>>& vector)
{
    std::vector<TUserObject*> result;
    result.reserve(vector.size());
    for (const auto& element : vector) {
        result.push_back(element.Get());
    }
    return result;
}

template <class TRpcPtr>
std::vector<TBlock> GetRpcAttachedBlocks(const TRpcPtr& rpc, bool validateChecksums)
{
    if (rpc->block_checksums_size() != 0 && std::ssize(rpc->Attachments()) != rpc->block_checksums_size()) {
        THROW_ERROR_EXCEPTION("Number of RPC attachments does not match the number of checksums")
            << TErrorAttribute("attachment_count", rpc->Attachments().size())
            << TErrorAttribute("checksum_count", rpc->block_checksums_size());
    }

    std::vector<TBlock> blocks;
    blocks.reserve(rpc->Attachments().size());
    for (int i = 0; i < std::ssize(rpc->Attachments()); ++i) {
        auto checksum = NullChecksum;
        if (rpc->block_checksums_size() != 0) {
            checksum = rpc->block_checksums(i);
        }

        blocks.emplace_back(rpc->Attachments()[i], checksum);

        if (validateChecksums) {
            auto error = blocks.back().CheckChecksum();
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Invalid block checksum in RPC attachment")
                    << TErrorAttribute("block_index", i)
                    << error;
            }
        }
    }

    return blocks;
}

template <class TRpcPtr>
void SetRpcAttachedBlocks(const TRpcPtr& rpc, const std::vector<TBlock>& blocks)
{
    rpc->Attachments().reserve(blocks.size());
    for (const auto& block : blocks) {
        rpc->Attachments().push_back(block.Data);
        rpc->add_block_checksums(block.Checksum);
    }
}

////////////////////////////////////////////////////////////////////////////////

TAllyReplicasInfo::operator bool() const
{
    return !Replicas.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
