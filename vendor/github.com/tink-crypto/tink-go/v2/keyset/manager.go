// Copyright 2019 Google LLC
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

package keyset

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/subtle/random"

	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

// entry is an internal representation of an [Entry].
type entry struct {
	key key.Key
	// A key ID is the ID requirement of the [key.Key] object, if set, otherwise a
	// random ID.
	fixedID    uint32
	hasFixedID bool
	status     KeyStatus
	isPrimary  bool
}

func fromKeysetEntries(entries []*Entry) []*entry {
	ret := make([]*entry, len(entries))
	for i, e := range entries {
		_, isRequired := e.key.IDRequirement()
		ret[i] = &entry{
			key:        e.key,
			fixedID:    e.keyID,
			hasFixedID: isRequired,
			status:     e.status,
			isPrimary:  e.isPrimary,
		}
	}
	return ret
}

// Manager manages a Keyset-proto, with convenience methods that rotate, disable, enable or destroy keys.
// Note: It is not thread-safe.
type Manager struct {
	entries           []*entry
	unavailableKeyIDs map[uint32]bool // set of key IDs that are not available for new keys
	annotations       map[string]string
}

// NewManager creates a new instance with an empty Keyset.
func NewManager() *Manager {
	ret := new(Manager)
	ret.entries = make([]*entry, 0)
	ret.unavailableKeyIDs = make(map[uint32]bool)
	return ret
}

// NewManagerFromHandle creates a new instance from the given Handle.
func NewManagerFromHandle(kh *Handle) *Manager {
	ret := new(Manager)
	ret.entries = fromKeysetEntries(kh.entries)
	ret.unavailableKeyIDs = make(map[uint32]bool)
	for _, e := range ret.entries {
		ret.unavailableKeyIDs[e.fixedID] = true
	}
	return ret
}

// Add generates and adds a fresh key using the given key template.
// the key is enabled on creation, but not set to primary.
// It returns the ID of the new key
func (km *Manager) Add(kt *tinkpb.KeyTemplate) (uint32, error) {
	if kt == nil {
		return 0, errors.New("keyset.Manager: key template is nil")
	}
	if kt.OutputPrefixType == tinkpb.OutputPrefixType_UNKNOWN_PREFIX {
		return 0, errors.New("keyset.Manager: unknown output prefix type")
	}
	if km.entries == nil {
		return 0, errors.New("keyset.Manager: cannot add key to nil keyset")
	}
	keyData, err := registry.NewKeyData(kt)
	if err != nil {
		return 0, fmt.Errorf("keyset.Manager: cannot create KeyData: %s", err)
	}
	keyID := km.newRandomKeyID()
	idRequirement := uint32(0)
	if kt.GetOutputPrefixType() != tinkpb.OutputPrefixType_RAW {
		idRequirement = keyID
	}
	keySerialization, err := protoserialization.NewKeySerialization(keyData, kt.OutputPrefixType, idRequirement)
	if err != nil {
		return 0, fmt.Errorf("keyset.Manager: cannot create KeySerialization: %s", err)
	}
	key, err := protoserialization.ParseKey(keySerialization)
	if err != nil {
		return 0, fmt.Errorf("keyset.Manager: cannot parse key: %s", err)
	}
	km.entries = append(km.entries, &entry{
		key:       key,
		isPrimary: false,
		fixedID:   keyID,
		status:    Enabled,
	})
	return keyID, nil
}

// KeyOpts is an interface for options that can be applied to a key.
type KeyOpts interface {
	apply(*entry) error
}

type keyOpts func(*entry) error

func (o keyOpts) apply(e *entry) error { return o(e) }

// WithStatus sets the status of the key.
func WithStatus(status KeyStatus) KeyOpts {
	return keyOpts(func(e *entry) error {
		e.status = status
		return nil
	})
}

// WithFixedID sets the ID of the key.
//
// NOTE: It is preferable to add keys with ID requirements or fixed IDs before
// adding keys without ID requirements to reduce the risk of ID collisions.
func WithFixedID(id uint32) KeyOpts {
	return keyOpts(func(e *entry) error {
		idReq, isRequired := e.key.IDRequirement()
		if isRequired && idReq != id {
			return fmt.Errorf("keyset.Manager: key requires ID %d, but WithFixedID was given ID %d", idReq, id)
		}
		e.fixedID = id
		e.hasFixedID = true
		return nil
	})
}

// AsPrimary sets the key as primary.
func AsPrimary() KeyOpts {
	return keyOpts(func(e *entry) error {
		e.isPrimary = true
		return nil
	})
}

// AddKeyWithOpts adds a key to the keyset with the given options.
//
// Default options are:
// - The key is enabled.
// - The key ID is random if the key does not require an ID.
//
// This is an internal API.
func (km *Manager) AddKeyWithOpts(key key.Key, _ internalapi.Token, opts ...KeyOpts) (uint32, error) {
	if key == nil {
		return 0, fmt.Errorf("keyset.Manager: key is nil")
	}

	// By default, use the key's ID requirement if it is set. Otherwise, use a
	// random ID.
	idReq, isRequired := key.IDRequirement()
	e := &entry{
		key:        key,
		fixedID:    idReq,
		hasFixedID: isRequired,
		status:     Enabled, // Default status is Enabled.
	}
	// Apply options.
	for _, opt := range opts {
		if err := opt.apply(e); err != nil {
			return 0, err
		}
	}

	if e.status == Unknown {
		return 0, fmt.Errorf("keyset.Manager: unknown key status")
	}
	if e.isPrimary {
		if e.status != Enabled {
			return 0, fmt.Errorf("keyset.Manager: primary key must be enabled")
		}
		// Make all others not primary.
		for _, otherEntry := range km.entries {
			otherEntry.isPrimary = false
		}
	}

	var keyID uint32
	if e.hasFixedID {
		// Use the fixed ID if not already in use.
		if _, found := km.unavailableKeyIDs[e.fixedID]; found {
			return 0, fmt.Errorf("keyset.Manager: key already has ID %d", e.fixedID)
		}
		keyID = e.fixedID
		km.unavailableKeyIDs[keyID] = true
	} else {
		// Use a random ID.
		keyID = km.newRandomKeyID()
	}
	e.fixedID = keyID
	km.entries = append(km.entries, e)
	return keyID, nil
}

// AddKey adds key to the keyset and returns the key ID. The added key is
// enabled by default.
func (km *Manager) AddKey(key key.Key) (uint32, error) {
	return km.AddKeyWithOpts(key, internalapi.Token{})
}

// AddNewKeyFromParameters generates a new key from parameters, adds the key to
// the keyset, and returns the key ID.
func (km *Manager) AddNewKeyFromParameters(parameters key.Parameters) (uint32, error) {
	keyTemplate, err := protoserialization.SerializeParameters(parameters)
	if err != nil {
		return 0, fmt.Errorf("keyset.Manager: %v", err)
	}
	return km.Add(keyTemplate)
}

func findEntry(entries []*entry, keyID uint32) (*entry, int, error) {
	i := slices.IndexFunc(entries, func(e *entry) bool {
		return e.fixedID == keyID
	})
	if i == -1 {
		return nil, i, fmt.Errorf("keyset.Manager: key with id %d not found", keyID)
	}
	return entries[i], i, nil
}

// SetPrimary sets the key with given keyID as primary.
// Returns an error if the key is not found or not enabled.
func (km *Manager) SetPrimary(keyID uint32) error {
	entry, _, err := findEntry(km.entries, keyID)
	if err != nil {
		return err
	}
	if entry.status != Enabled {
		return errors.New("keyset.Manager: cannot set key as primary because it's not enabled")
	}
	entry.isPrimary = true
	// Make all others not primary.
	for _, otherEntry := range km.entries {
		if otherEntry.fixedID == entry.fixedID {
			continue
		}
		otherEntry.isPrimary = false
	}
	return nil
}

// Enable will enable the key with given keyID.
// Returns an error if the key is not found or is not enabled or disabled already.
func (km *Manager) Enable(keyID uint32) error {
	entry, _, err := findEntry(km.entries, keyID)
	if err != nil {
		return err
	}
	if entry.status != Disabled && entry.status != Enabled {
		return fmt.Errorf("keyset.Manager: cannot enable key with id %d with status %s", keyID, entry.status)
	}
	entry.status = Enabled
	return nil
}

// Disable will disable the key with given keyID.
// Returns an error if the key is not found or it is the primary key.
func (km *Manager) Disable(keyID uint32) error {
	entry, _, err := findEntry(km.entries, keyID)
	if err != nil {
		return err
	}
	if entry.isPrimary {
		return errors.New("keyset.Manager: cannot disable the primary key")
	}
	if entry.status != Enabled && entry.status != Disabled {
		return fmt.Errorf("keyset.Manager: cannot disable key with id %d with status %s", keyID, entry.status)
	}
	entry.status = Disabled
	return nil
}

// Delete will delete the key with given keyID, removing the key from the keyset entirely.
// Returns an error if the key is not found or it is the primary key.
func (km *Manager) Delete(keyID uint32) error {
	entry, i, err := findEntry(km.entries, keyID)
	if err != nil {
		return err
	}
	if entry.isPrimary {
		return errors.New("keyset.Manager: cannot delete the primary key")
	}
	km.entries = slices.Delete(km.entries, i, i+1)
	return nil
}

// SetAnnotations sets the annotations for the keyset.
//
// This method makes a copy of the annotations map to prevent the caller from
// modifying the annotations.
func (km *Manager) SetAnnotations(annotations map[string]string) error {
	if km == nil {
		return errors.New("keyset.Manager: key manager is nil")
	}
	km.annotations = maps.Clone(annotations)
	return nil
}

// Handle creates a new Handle for the managed keyset.
func (km *Manager) Handle() (*Handle, error) {
	entries := make([]*Entry, len(km.entries))
	for i, e := range km.entries {
		entries[i] = newUnmonitoredEntry(e.key, e.isPrimary, e.fixedID, e.status)
	}
	return newFromEntries(entries, WithAnnotations(km.annotations))
}

// newRandomKeyID generates a key id that has not been used by any key in the keyset.
func (km *Manager) newRandomKeyID() uint32 {
	for {
		newRandomID := random.GetRandomUint32()
		if _, found := km.unavailableKeyIDs[newRandomID]; !found {
			km.unavailableKeyIDs[newRandomID] = true
			return newRandomID
		}
	}
}
