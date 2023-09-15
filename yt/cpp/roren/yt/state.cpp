#include "state.h"

void TSerializer<NRoren::NPrivate::TYtStateVtable>::Save(IOutputStream* output, const TYtStateVtable& stateVtable)
{
    ::Save(output, stateVtable.StateTKVvtable);
    ::Save(output, reinterpret_cast<ui64>(stateVtable.LoadState));
    ::Save(output, reinterpret_cast<ui64>(stateVtable.SaveState));
    ::Save(output, reinterpret_cast<ui64>(stateVtable.StateFromKey));
    ::Save(output, reinterpret_cast<ui64>(stateVtable.StateFromTKV));
}

void TSerializer<NRoren::NPrivate::TYtStateVtable>::Load(IInputStream* input, TYtStateVtable& stateVtable)
{
    ::Load(input, stateVtable.StateTKVvtable);
    ::Load(input, reinterpret_cast<ui64&>(stateVtable.LoadState));
    ::Load(input, reinterpret_cast<ui64&>(stateVtable.SaveState));
    ::Load(input, reinterpret_cast<ui64&>(stateVtable.StateFromKey));
    ::Load(input, reinterpret_cast<ui64&>(stateVtable.StateFromTKV));
}

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
    TTypeTag<TYtStateVtable> YtStateVtableTag{"yt_state_vtable"};
    TTypeTag<TString> YtStateInPathTag{"yt_state_in_path"};
    TTypeTag<TString> YtStateOutPathTag{"yt_state_out_path"};

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

