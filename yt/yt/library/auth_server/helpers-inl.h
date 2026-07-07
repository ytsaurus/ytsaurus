#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class TAuthenticator, class TCredentials, class TResult>
class TCompositeAuthSession
    : public TRefCounted
{
public:
    TCompositeAuthSession(
        std::vector<TIntrusivePtr<TAuthenticator>> authenticators,
        TCredentials credentials,
        TError failureError)
        : Authenticators_(std::move(authenticators))
        , Credentials_(std::move(credentials))
        , FailureError_(std::move(failureError))
    {
        InvokeNext();
    }

    TFuture<TResult> GetResult()
    {
        return Promise_;
    }

private:
    const std::vector<TIntrusivePtr<TAuthenticator>> Authenticators_;
    const TCredentials Credentials_;
    const TError FailureError_;

    TPromise<TResult> Promise_ = NewPromise<TResult>();
    std::vector<TError> Errors_;
    int CurrentIndex_ = 0;

    void InvokeNext()
    {
        if (CurrentIndex_ >= std::ssize(Authenticators_)) {
            Promise_.Set(FailureError_ << Errors_);
            return;
        }

        Authenticators_[CurrentIndex_++]->Authenticate(Credentials_).Subscribe(
            BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TResult>& result) {
                if (result.IsOK()) {
                    Promise_.Set(result.Value());
                } else {
                    Errors_.push_back(result);
                    InvokeNext();
                }
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> GetByYPath(const NYTree::INodePtr& node, const NYPath::TYPath& path)
{
    try {
        auto child = NYTree::FindNodeByYPath(node, path);
        if (!child) {
            return TError("Missing %v", path);
        }
        return NYTree::ConvertTo<T>(std::move(child));
    } catch (const std::exception& ex) {
        return TError("Unable to extract %v", path) << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
