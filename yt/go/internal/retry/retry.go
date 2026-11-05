package retry

import (
	"errors"
	"net"
	"net/http"

	"go.ytsaurus.tech/yt/go/yterrors"
)

const codeBalancerServiceUnavailable yterrors.ErrorCode = 1_000_000

func hasRetriableCode(err error) bool {
	var ytErr *yterrors.Error
	if !errors.As(err, &ytErr) {
		return false
	}

	codes := yterrors.GetErrorCodes(ytErr)
	for code := range codes {
		switch code {
		case yterrors.CodeCanceled,
			yterrors.CodeTimeout,
			yterrors.CodeTransportError,
			yterrors.CodeUnavailable,
			yterrors.CodeRPCRequestQueueSizeLimitExceeded,
			yterrors.CodeTransientFailure,
			yterrors.CodeMasterDisconnected,
			yterrors.CodeRequestQueueSizeLimitExceeded,
			yterrors.CodeRetriableArchiveError:
			return true
		}
	}
	return isRetriableChunkError(codes)
}

// IsRetriableError reports whether a request that is safe to repeat can be retried after err; the caller checks proxy bans first.
func IsRetriableError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		var lookupErr *net.DNSError
		if errors.As(err, &lookupErr) && lookupErr.IsNotFound {
			return false
		}
		if tcp, ok := opErr.Addr.(*net.TCPAddr); ok && tcp.IP.IsLoopback() {
			return false
		}
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return IsRetriableHTTPResponseError(err)
}

// IsRetriableHTTPResponseError reports whether an HTTP response error is retriable.
func IsRetriableHTTPResponseError(err error) bool {
	if yterrors.ContainsErrorCode(err, codeBalancerServiceUnavailable) || hasRetriableCode(err) {
		return true
	}

	var httpErr *yterrors.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
	}

	return false
}

// IsProxyBannedError reports whether err says that the proxy is banned.
func IsProxyBannedError(err error) bool {
	// COMPAT(babenko): drop ProxyBanned in favor of PeerBanned.
	if yterrors.ContainsErrorCode(err, yterrors.CodeProxyBanned) ||
		yterrors.ContainsErrorCode(err, yterrors.CodePeerBanned) {
		return true
	}

	var ytErr *yterrors.Error
	return errors.As(err, &ytErr) && ytErr.Message == "Proxy is banned"
}

// IsRetriableRPCError reports whether an RPC error is retriable.
func IsRetriableRPCError(err error) bool {
	if hasRetriableCode(err) {
		return true
	}

	var ytErr *yterrors.Error
	if !errors.As(err, &ytErr) {
		return false
	}

	switch ytErr.Code {
	case yterrors.CodeProtocolError,
		yterrors.CodeNoSuchService,
		yterrors.CodeNoSuchMethod,
		yterrors.CodeNoSuchRealm,
		yterrors.CodeSslError:
		return true
	}

	return false
}

// IsRetriableChunkError reports whether err is a retriable chunk error.
func IsRetriableChunkError(err error) bool {
	return isRetriableChunkError(yterrors.GetErrorCodes(err))
}

// IsMutationResultUnknown reports whether a mutation may have been applied despite err.
func IsMutationResultUnknown(err error) bool {
	var ytErr *yterrors.Error
	if !errors.As(err, &ytErr) {
		return true
	}

	return yterrors.ContainsErrorCode(err, yterrors.CodeTransportError) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeTimeout) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeUnavailable) ||
		yterrors.ContainsErrorCode(err, codeBalancerServiceUnavailable)
}

func isRetriableChunkError(codes map[yterrors.ErrorCode]struct{}) bool {
	hasChunkError := false
	for code := range codes {
		switch code {
		case yterrors.CodeSessionAlreadyExists,
			yterrors.CodeChunkAlreadyExists,
			yterrors.CodeWindowError,
			yterrors.CodeBlockContentMismatch,
			yterrors.CodeInvalidBlockChecksum,
			yterrors.CodeMalformedReadRequest,
			yterrors.CodeMissingExtension,
			yterrors.CodeNoSuchBlock,
			yterrors.CodeNoSuchChunk,
			yterrors.CodeNoSuchChunkList,
			yterrors.CodeNoSuchChunkTree,
			yterrors.CodeNoSuchChunkView,
			yterrors.CodeNoSuchMedium:
			return false
		}
		if code >= 700 && code < 800 {
			hasChunkError = true
		}
	}
	return hasChunkError
}
