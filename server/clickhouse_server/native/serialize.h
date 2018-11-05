#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/yson/public.h>
#include <yt/core/ytree/public.h>

namespace NYT {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TExtension;
class TExtensionSet;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TExtension& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TExtension& spec, NYTree::INodePtr node);

void Serialize(const TExtensionSet& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TExtensionSet& spec, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NProto

namespace NChunkClient {

class TDataSource;
class TDataSourceDirectory;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDataSource& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TDataSource& spec, NYTree::INodePtr node);

void Serialize(const TDataSourceDirectory& sourceDirectory, NYson::IYsonConsumer* consumer);
void Deserialize(TDataSourceDirectory& sourceDirectory, NYTree::INodePtr node);

void Serialize(const TDataSliceDescriptor& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TDataSliceDescriptor& spec, NYTree::INodePtr node);

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TChunkSpec& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TChunkSpec& spec, NYTree::INodePtr node);

void Serialize(const TChunkMeta& spec, NYson::IYsonConsumer* consumer);
void Deserialize(TChunkMeta& spec, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NProto
}   // namespace NChunkClient

namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TNodeDirectory& nodeDirectory, NYson::IYsonConsumer* consumer);
void Deserialize(TNodeDirectory& nodeDirectory, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NNodeTrackerClient
}   // namespace NYT
