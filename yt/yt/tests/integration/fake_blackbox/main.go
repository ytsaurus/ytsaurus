package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

var (
	flagPort = flag.Int("port", 0, "")
)

func replyOKToken(w http.ResponseWriter, login string) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{
        "karma" : {
           "value" : 0
        },
        "karma_status" : {
           "value" : 0
        },
        "uid" : {
           "hosted" : false,
           "lite" : false,
           "value" : "1120000000007984"
        },
        "error" : "OK",
        "status" : {
           "value" : "VALID",
           "id" : 0
        },
        "have_password" : true,
        "login" : %q,
        "attributes" : {
            "1008" : %q
        },
        "have_hint" : false,
        "connection_id" : "t:596886",
        "oauth" : {
           "device_id" : "",
           "client_icon" : "",
           "uid" : "1120000000007984",
           "issue_time" : "2021-08-28 20:18:55",
           "token_id" : "596886",
           "device_name" : "",
           "client_id" : "9a175374bf104366a92b126acf8acdd0",
           "client_ctime" : "2018-09-03 21:09:21",
           "is_ttl_refreshable" : false,
           "xtoken_id" : "",
           "meta" : "",
           "client_homepage" : "",
           "client_name" : "YT",
           "ctime" : "2019-03-14 20:26:46",
           "client_is_yandex" : false,
           "expire_time" : null,
           "scope" : "yt:api"
        }
     }`, login, login)
}

func replyOKUserTicket(w http.ResponseWriter, login string) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{
        "users" : [
            {
                "login": %q
            }
        ]
    }`, login)
}

func replyInternalError(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, `{
        "exception": {
            "value": "ACCESS_DENIED",
            "id": 21
        },
        "error": "Access denied by fake_blackbox"
    }`)
}

func replyExpiredToken(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, `{
        "error" : "expired_token",
        "status" : {
           "value" : "INVALID",
           "id" : 5
        }
    }`)
}

func main() {
	flag.Parse()

	http.HandleFunc("/blackbox", func(w http.ResponseWriter, r *http.Request) {
		switch r.FormValue("method") {
		case "oauth":
			if token := r.FormValue("oauth_token"); token != "" {
				switch token {
				case "bb_token":
					replyOKToken(w, "prime")
				default:
					replyExpiredToken(w)
				}

				return
			}

		case "user_ticket":
			replyOKUserTicket(w, "prime")
			return
		}

		replyInternalError(w)
	})

	log.Fatal(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *flagPort), nil))
}
