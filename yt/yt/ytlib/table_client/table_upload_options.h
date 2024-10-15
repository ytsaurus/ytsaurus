#pragma once

// NB(arkady-e1ppa): This is a band-aid fix of ytlib dep of distributed table write client part
// We moved these options to table_upload_options without changing anything in actual public includes
// therefore some files that include this one might have relied on ytlib public.h files inclusions.
// in order to neither fix nor disrupt that, we manually include these "maybe required" files here.
#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/table_upload_options.h>
