#pragma once

#include "common.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

class TStreamSaveContext;
class TStreamLoadContext;

template <class TSaveContext, class TLoadContext>
class TCustomPersistenceContext;

struct TValueBoundComparer;
struct TValueBoundSerializer;

template <class T, class C, class = void>
struct TSerializerTraits;

class TChunkedMemoryPool;

template <class TKey, class TComparer>
class TSkipList;

class TBlobOutput;
class TFakeStringBufStore;

class TStringBuilder;

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
