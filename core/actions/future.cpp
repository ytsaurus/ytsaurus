#include "future.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const TFuture<void> VoidFuture = MakeWellKnownFuture(TError());
const TFuture<bool> TrueFuture = MakeWellKnownFuture(TErrorOr<bool>(true));
const TFuture<bool> FalseFuture = MakeWellKnownFuture(TErrorOr<bool>(false));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
