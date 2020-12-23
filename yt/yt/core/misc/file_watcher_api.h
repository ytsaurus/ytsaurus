#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Keep in sync with IN_* in inotify.h.
DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EFileWatchMode, ui32,
    ((Access)      (0x00000001)) // File was accessed.
    ((Modify)      (0x00000002)) // File was modified.
    ((Attributes)  (0x00000004)) // Metadata changed.
    ((CloseWrite)  (0x00000008)) // Writtable file was closed.
    ((CloseNowrite)(0x00000010)) // Unwrittable file closed.
    ((Close)       (0x00000018)) // Close.
    ((Open)        (0x00000020)) // File was opened.
    ((MovedForm)   (0x00000040)) // File was moved from X.
    ((MovedTo)     (0x00000080)) // File was moved to Y.
    ((Move)        (0x000000c0)) // Moves.
    ((Create)      (0x00000100)) // Subfile was created.
    ((Delete)      (0x00000200)) // Subfile was deleted.
    ((DeleteSelf)  (0x00000400)) // Self was deleted.
    ((MoveSelf)    (0x00000800)) // Self was moved.
);

DECLARE_REFCOUNTED_STRUCT(IFileWatcher)
DECLARE_REFCOUNTED_CLASS(TFileWatch)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
