package rpcclient

import (
	"encoding/json"
	"net/http"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yterrors"
)

// unexpectedStatusCode is last effort attempt to get useful error message from a failed request.
func unexpectedStatusCode(rsp *http.Response) error {
	d := json.NewDecoder(rsp.Body)
	d.UseNumber()

	var ytErr yterrors.Error
	if err := d.Decode(&ytErr); err == nil {
		return &ytErr
	}

	return xerrors.Errorf("unexpected status code %d", rsp.StatusCode)
}
