#include "stdafx.h"
#include "future.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const TFuture<void> VoidFuture = MakeFuture(TError());
const TFuture<bool> TrueFuture = MakeFuture(true);
const TFuture<bool> FalseFuture = MakeFuture(false);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
