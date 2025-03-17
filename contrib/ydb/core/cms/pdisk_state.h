#pragma once

#include <contrib/ydb/core/protos/node_whiteboard.pb.h>
#include <contrib/ydb/core/protos/blobstorage_disk.pb.h>

namespace NKikimr::NCms {

using TPDiskStateInfo = NKikimrWhiteboard::TPDiskStateInfo;
using EPDiskState = NKikimrBlobStorage::TPDiskState::E;

} // namespace NKikimr::NCms
