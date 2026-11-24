#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <map>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Attribute names starting with this prefix are owned by flow itself.
inline const std::string ReservedFlowAttributeNamePrefix = "_ytflow_";

//! Attribute names starting with this prefix are interpreted by YDB.
inline const std::string ReservedYdbAttributeNamePrefix = "__";

//! The attribute YDB's Kafka API stores the record key in: a reader over the Kafka protocol
//! gets it as the key rather than as a header. It is exempt from
//! #ReservedYdbAttributeNamePrefix so that a pipeline can produce keyed records.
inline const std::string KafkaKeyAttributeName = "__key";

//! The attribute #TFlowQueueMeta is written under.
inline const std::string DefaultFlowQueueMetaName = ReservedFlowAttributeNamePrefix + "queue_meta";

//! The name used before the prefix was reserved. The source still understands it, so that a
//! reader can be updated before the writers of its topic. The other order loses the flow queue
//! meta: nothing teaches an old source #DefaultFlowQueueMetaName.
inline const std::string LegacyFlowQueueMetaName = "flow_queue_meta";

//! YDB's limits on a topic message's metadata: see the |metadata_items| field of
//! StreamWriteMessage.WriteRequest.MessageData in ydb_topic.proto, which declares the item
//! count as an annotation and states the byte cap in the comment above it.
//! #TYdbTopicAttributesTest.ItemCountMatchesYdb keeps the count in sync.
constexpr i64 MaxYdbMessageMetaSize = 4096;
constexpr i64 MaxYdbMessageMetaItemCount = 1000;

//! Attribute names a pipeline may neither write nor see in the source's attributes column.
bool IsReservedYdbTopicAttributeName(const std::string& name);

//! Throws if the names are reserved or the attributes do not fit into the topic limits.
void ValidateYdbTopicAttributes(const TYdbTopicAttributes& attributes);

using TYdbMessageMeta = std::vector<std::pair<std::string, std::string>>;

//! Throws if |meta| does not fit into the topic limits.
void ValidateYdbMessageMeta(const TYdbMessageMeta& meta);

//! Packs |attributes| into the value of the sink's attributes column.
//! Validates globally reserved names and the absolute topic limits. A sink additionally
//! validates the result combined with its configured flow queue meta.
NYson::TYsonString PackYdbTopicAttributes(const TYdbTopicAttributes& attributes);

//! Reads back a value produced by #PackYdbTopicAttributes() or written into the source's
//! attributes column.
TYdbTopicAttributes UnpackYdbTopicAttributes(NYson::TYsonStringBuf value);

//! Reads the value of the sink's attributes column: null means no attributes, anything the
//! sink may not write throws. Validates again because the column need not come from
//! #PackYdbTopicAttributes().
TYdbTopicAttributes ParseYdbTopicAttributesColumn(std::optional<NYson::TYsonStringBuf> value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
