#include "base_state.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TTypeTag<TYtStateVtable> YtStateVtableTag{"yt_state_vtable"};
TTypeTag<TString> YtStateInPathTag{"yt_state_in_path"};
TTypeTag<TString> YtStateOutPathTag{"yt_state_out_path"};

void InitializeYtPState(TRawPStateNodePtr rawPState, TString inStatePath, TString outStatePath, TYtStateVtable stateVtable)
{
    if (outStatePath.empty()) {
        outStatePath = inStatePath;
    }

    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateVtableTag, std::move(stateVtable));
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateInPathTag, std::move(inStatePath));
    NPrivate::SetAttribute(*rawPState, NPrivate::YtStateOutPathTag, std::move(outStatePath));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
