#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TStaticTableExportParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicStaticTableExportParameters);
DECLARE_REFCOUNTED_STRUCT(TStaticTableExportWriterParameters);
DECLARE_REFCOUNTED_STRUCT(TStaticTableExportPart);
DECLARE_REFCOUNTED_STRUCT(TStaticTableExportCursor);
DECLARE_REFCOUNTED_STRUCT(TStaticTableExportProgress);
DECLARE_REFCOUNTED_CLASS(TStaticTableExportWindowStream);
DECLARE_REFCOUNTED_CLASS(TStaticTableExportWriterStorage);
DECLARE_REFCOUNTED_CLASS(TStaticTableExportPublisher);
DECLARE_REFCOUNTED_STRUCT(IStaticTableExportParticipant);
DECLARE_REFCOUNTED_CLASS(TStaticTableExportWriterResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
