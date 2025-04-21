#include "schema.h"

#include <yql/essentials/sql/v1/complete/text/case.h>

#include <util/charset/utf8.h>

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
                EraseIf(entries, [prefix = ToLowerUTF8(prefix)](const TFolderEntry& entry) {
                    return !entry.Name.StartsWith(prefix);
                });

                for (TFolderEntry& entry : entries) {
                    if (path.empty()) {
                        entry.Name.prepend('/');
                    }
                }

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
