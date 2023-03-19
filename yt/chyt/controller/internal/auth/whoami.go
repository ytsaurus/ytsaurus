package auth

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func ContainsUnauthorized(err error) bool {
	return yterrors.ContainsErrorCode(err, yterrors.CodeInvalidCredentials) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeAuthenticationError)
}

func WhoAmI(proxy string, token string) (username string, err error) {
	address := yt.NormalizeProxyURL(proxy, false /*tvmOnly*/, 0 /*tvmOnlyPort*/).Address
	url := address + "/auth/whoami"
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Authorization", "OAuth "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		var result struct {
			Login string `json:"login"`
		}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return "", err
		}
		return result.Login, nil

	case http.StatusUnauthorized:
		var authError yterrors.Error
		err = json.Unmarshal(body, &authError)
		if err != nil {
			return "", err
		}
		if !ContainsUnauthorized(&authError) {
			// NOTE: actually should never happen.
			return "", yterrors.Err(yterrors.CodeAuthenticationError, "authentication failed", authError)
		}
		return "", &authError

	default:
		return "", yterrors.Err("unexpected http status code",
			yterrors.Attr("status_code", resp.StatusCode))
	}
}
