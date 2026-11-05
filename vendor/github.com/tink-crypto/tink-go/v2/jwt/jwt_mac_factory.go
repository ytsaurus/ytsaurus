// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jwt

import (
	"fmt"

	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/internalregistry"
	"github.com/tink-crypto/tink-go/v2/internal/monitoringutil"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveset"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/monitoring"
)

type macAndKeyID struct {
	mac   MAC
	keyID uint32
}

// NewMAC generates a new [jwt.MAC] primitive with the global registry.
func NewMAC(handle *keyset.Handle) (MAC, error) {
	return NewMACWithConfig(handle, &registryconfig.RegistryConfig{})
}

// NewMACWithConfig generates a new [jwt.MAC] primitive with the provided
// [keyset.Config].
func NewMACWithConfig(handle *keyset.Handle, config keyset.Config) (MAC, error) {
	if handle == nil {
		return nil, fmt.Errorf("jwt_mac_factory: keyset handle can't be nil")
	}
	ps, err := keyset.Primitives[MAC](handle, config, internalapi.Token{})
	if err != nil {
		return nil, fmt.Errorf("jwt_mac_factory: cannot obtain primitive set: %v", err)
	}
	return newWrappedJWTMAC(ps)
}

func newWrappedJWTMAC(ps *primitiveset.PrimitiveSet[MAC]) (*wrappedJWTMAC, error) {
	var macs []macAndKeyID
	var primary macAndKeyID
	var computeLogger monitoring.Logger
	var verifyLogger monitoring.Logger
	computeLogger, verifyLogger, err := createMacLoggers(ps)
	if err != nil {
		return nil, err
	}
	for _, primitives := range ps.Entries {
		for _, p := range primitives {
			// Only full primitives are supported.
			if p.FullPrimitive == nil {
				// Something is wrong, this should not happen.
				return nil, fmt.Errorf("jwt_mac_factory: full primitive is nil")
			}
			macs = append(macs, macAndKeyID{
				mac:   p.FullPrimitive,
				keyID: p.KeyID,
			})
		}
	}
	primary = macAndKeyID{
		mac:   ps.Primary.FullPrimitive,
		keyID: ps.Primary.KeyID,
	}
	return &wrappedJWTMAC{
		macs:          macs,
		primary:       primary,
		computeLogger: computeLogger,
		verifyLogger:  verifyLogger,
	}, nil
}

// wrappedJWTMAC is a JWT MAC implementation that uses the underlying primitive
// set for JWT MAC.
type wrappedJWTMAC struct {
	macs          []macAndKeyID
	primary       macAndKeyID
	computeLogger monitoring.Logger
	verifyLogger  monitoring.Logger
}

var _ MAC = (*wrappedJWTMAC)(nil)

func createMacLoggers[T any](ps *primitiveset.PrimitiveSet[T]) (monitoring.Logger, monitoring.Logger, error) {
	if len(ps.Annotations) == 0 {
		return &monitoringutil.DoNothingLogger{}, &monitoringutil.DoNothingLogger{}, nil
	}
	client := internalregistry.GetMonitoringClient()
	keysetInfo, err := monitoringutil.KeysetInfoFromPrimitiveSet(ps)
	if err != nil {
		return nil, nil, err
	}
	computeLogger, err := client.NewLogger(&monitoring.Context{
		Primitive:   "jwtmac",
		APIFunction: "compute",
		KeysetInfo:  keysetInfo,
	})
	if err != nil {
		return nil, nil, err
	}
	verifyLogger, err := client.NewLogger(&monitoring.Context{
		Primitive:   "jwtmac",
		APIFunction: "verify",
		KeysetInfo:  keysetInfo,
	})
	if err != nil {
		return nil, nil, err
	}
	return computeLogger, verifyLogger, nil
}

func (w *wrappedJWTMAC) ComputeMACAndEncode(token *RawJWT) (string, error) {
	signedToken, err := w.primary.mac.ComputeMACAndEncode(token)
	if err != nil {
		w.computeLogger.LogFailure()
		return "", err
	}
	w.computeLogger.Log(w.primary.keyID, 1)
	return signedToken, nil
}

func (w *wrappedJWTMAC) VerifyMACAndDecode(compact string, validator *Validator) (*VerifiedJWT, error) {
	var interestingErr error
	for _, macWithKeyID := range w.macs {
		mac, keyID := macWithKeyID.mac, macWithKeyID.keyID
		verifiedJWT, err := mac.VerifyMACAndDecode(compact, validator)
		if err == nil {
			w.verifyLogger.Log(keyID, 1)
			return verifiedJWT, nil
		}
		if err != errJwtVerification {
			// Any error that is not the generic errJwtVerification is considered
			// interesting.
			interestingErr = err
		}
	}
	w.verifyLogger.LogFailure()
	if interestingErr != nil {
		return nil, interestingErr
	}
	return nil, errJwtVerification
}
