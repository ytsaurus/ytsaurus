#include "profile_state.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

NYT::TNode TYtStateVtable::SerializableToNode(const TYtStateVtable& stateVtable)
{
    TStringStream result;
    TSerializer<TYtStateVtable>::Save(&result, stateVtable);
    return result.Str();
}

TYtStateVtable TYtStateVtable::SerializableFromNode(const NYT::TNode& node)
{
    TStringStream input(node.AsString());
    TYtStateVtable result;
    TSerializer<TYtStateVtable>::Load(&input, result);
    return result;
}

}  // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

