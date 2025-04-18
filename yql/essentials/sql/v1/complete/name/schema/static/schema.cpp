#include "schema.h"

namespace NSQLComplete {

    namespace {

        class TSchema: public ISchema {
        public:
            explicit TSchema(THashMap<TPath, TVector<TFolderEntry>> data)
                : Data_(std::move(data))
            {
            }

            NThreading::TFuture<TListResponse> List(const TListRequest& request) override {
                auto [path, prefix] = ParsePath(request.Path);

                TVector<TFolderEntry> entries = Data_[path];
                // TODO(YQL-19747): filtration by the prefix.

                TListResponse response = {
                    .NameHintLength = prefix.size(),
                    .Entries = std::move(entries),
                };

                return NThreading::MakeFuture(std::move(response));
            }

        private:
            static std::tuple<TStringBuf, TStringBuf> ParsePath(TPath path Y_LIFETIME_BOUND) {
                size_t pos = path.find_last_of('/');
                if (pos == TString::npos) {
                    return {"", path};
                }

                TStringBuf head, tail;
                TStringBuf(path).SplitAt(pos + 1, head, tail);
                return {head, tail};
            }

            THashMap<TPath, TVector<TFolderEntry>> Data_;
        };

    } // namespace

    ISchema::TPtr MakeStaticSchema(THashMap<TPath, TVector<TFolderEntry>> data) {
        return MakeHolder<TSchema>(std::move(data));
    }

} // namespace NSQLComplete
