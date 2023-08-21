package httpserver

import (
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type HTTPResponder struct {
	Logger log.Logger
}

func NewHTTPResponder(l log.Logger) HTTPResponder {
	return HTTPResponder{
		Logger: l,
	}
}

func (a HTTPResponder) Reply(w http.ResponseWriter, status int, rsp any) {
	body, err := yson.Marshal(rsp)
	if err != nil {
		a.Logger.Error("failed to marshal response", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header()["Content-Type"] = []string{"application/yson"}
	w.WriteHeader(status)
	_, err = w.Write(body)
	if err != nil {
		a.Logger.Error("failed to write response body", log.Error(err))
	}
}

func (a HTTPResponder) ReplyWithError(w http.ResponseWriter, err error) {
	a.Reply(w, http.StatusBadRequest, map[string]any{
		"to_print": err.Error(),
		"error":    yterrors.FromError(err),
	})
}

func (a HTTPResponder) ReplyOK(w http.ResponseWriter, rsp any) {
	a.Reply(w, http.StatusOK, rsp)
}
