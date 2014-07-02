#pragma once

#include "common.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

class TStreamSaveContext;
class TStreamLoadContext;

struct TValueBoundComparer;
struct TValueBoundSerializer;

template <class T, class C, class = void>
struct TSerializerTraits;

class TChunkedMemoryPool;

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;
class TFakeStringBufStore;

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
