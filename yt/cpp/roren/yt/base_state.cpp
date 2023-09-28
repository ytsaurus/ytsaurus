#include "base_state.h"

namespace NRoren::NPrivate
{
TTypeTag<TYtStateVtable> YtStateVtableTag{"yt_state_vtable"};
TTypeTag<TString> YtStateInPathTag{"yt_state_in_path"};
TTypeTag<TString> YtStateOutPathTag{"yt_state_out_path"};

void InitializeYtPState(TRawPStateNodePtr rawPState, TString in_state_path, TString out_state_path, TYtStateVtable stateVtable)
{
    if (out_state_path.empty()) {
        out_state_path = in_state_path;
    }

    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateVtableTag, std::move(stateVtable));
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateInPathTag, std::move(in_state_path));
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateOutPathTag, std::move(out_state_path));
}

}  // namespace NRoren::NPrivate

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
