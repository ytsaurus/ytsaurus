package ytmsvc

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/rs/cors"

	"go.ytsaurus.tech/yt/go/yterrors"
)

type HTTPHandlerE func(w http.ResponseWriter, req *http.Request) (result any, err error)

var ResponseStatusOK any = struct {
	Status string `json:"status"`
}{Status: "ok"}

func WriteError(w http.ResponseWriter, req *http.Request, err error) {
	err = yterrors.FromError(err)
	w.WriteHeader(http.StatusBadRequest)
	encoder := json.NewEncoder(w)
	Must0(encoder.Encode(err))
}

func FormatResponse(handler HTTPHandlerE) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		result, err := handler(w, req)
		if err != nil {
			WriteError(w, req, err)
			return
		}
		encoder := json.NewEncoder(w)
		err = encoder.Encode(result)
		if err != nil {
			WriteError(w, req, err)
			return
		}
	}
}

func CORS(conf *CORSConfig) func(next http.Handler) http.Handler {
	c := cors.New(cors.Options{
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Origin", "Accept", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders: []string{"Content-Disposition"},
		AllowOriginFunc: func(origin string) bool {
			u, err := url.Parse(origin)
			if err != nil {
				return false
			}

			for _, h := range conf.AllowedHosts {
				if u.Host == h {
					return true
				}
			}

			for _, s := range conf.AllowedHostSuffixes {
				if strings.HasSuffix(u.Host, s) {
					return true
				}
			}

			return false
		},
		AllowCredentials: true,
	})

	return func(next http.Handler) http.Handler {
		return c.Handler(next)
	}
}

type CORSConfig struct {
	// Allowed hosts is a list of allowed hostnames checked via exact match.
	AllowedHosts []string `yaml:"allowed_hosts"`
	// Allowed hosts is a list of allowed hostname suffixes checked via HasSuffix function.
	AllowedHostSuffixes []string `yaml:"allowed_host_suffixes"`
}
