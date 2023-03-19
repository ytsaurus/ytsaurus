package auth

import (
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

// Auth returns a middleware that validates yt authorization via provided oauth token.
//
// User's normalized login is added to request context.
func Auth(proxy string, disableAuth bool, l log.Structured) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var user string

			if disableAuth {
				user = r.Header.Get("X-YT-TestUser")
				ctxlog.Info(r.Context(), l.Logger(), "authentication is disabled, using X-YT-TestUser header",
					log.String("user", user))
			} else {
				token, err := GetTokenFromHeader(r)
				if err != nil {
					ctxlog.Error(r.Context(), l.Logger(), "failed to get token from headers", log.Error(err))
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}

				user, err = WhoAmI(proxy, token)
				if err != nil {
					ctxlog.Error(r.Context(), l.Logger(), "user authentication failed", log.Error(err))

					if ContainsUnauthorized(err) {
						http.Error(w, "Unauthorized", http.StatusUnauthorized)
					} else {
						http.Error(w, "User authentication failed", http.StatusInternalServerError)
					}
					return
				}

				ctxlog.Info(r.Context(), l.Logger(), "user authenticated via cluster proxy",
					log.String("user", user))
			}

			ctx := WithRequester(r.Context(), user)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
