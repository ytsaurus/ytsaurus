package cache

import (
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/yandex/tvm"
)

const (
	Hit Status = iota
	Miss
	GonnaMissy
)

type (
	Status int

	Cache struct {
		ttl     time.Duration
		maxTTL  time.Duration
		tickets map[tvm.ClientID]Entry
		aliases map[string]tvm.ClientID
		lock    sync.RWMutex
	}

	Entry struct {
		Value string
		Born  time.Time
	}
)

func New(ttl, maxTTL time.Duration) *Cache {
	return &Cache{
		ttl:     ttl,
		maxTTL:  maxTTL,
		tickets: make(map[tvm.ClientID]Entry, 1),
		aliases: make(map[string]tvm.ClientID, 1),
	}
}

func (c *Cache) Gc() {
	now := time.Now()

	c.lock.Lock()
	defer c.lock.Unlock()
	for clientID, ticket := range c.tickets {
		if ticket.Born.Add(c.maxTTL).After(now) {
			continue
		}

		delete(c.tickets, clientID)
		for alias, aClientID := range c.aliases {
			if clientID == aClientID {
				delete(c.aliases, alias)
			}
		}
	}
}

func (c *Cache) ClientIDs() []tvm.ClientID {
	c.lock.RLock()
	defer c.lock.RUnlock()

	clientIDs := make([]tvm.ClientID, 0, len(c.tickets))
	for clientID := range c.tickets {
		clientIDs = append(clientIDs, clientID)
	}
	return clientIDs
}

func (c *Cache) Aliases() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	aliases := make([]string, 0, len(c.aliases))
	for alias := range c.aliases {
		aliases = append(aliases, alias)
	}
	return aliases
}

func (c *Cache) Load(clientID tvm.ClientID) (string, Status) {
	c.lock.RLock()
	e, ok := c.tickets[clientID]
	c.lock.RUnlock()
	if !ok {
		return "", Miss
	}

	now := time.Now()
	exp := e.Born.Add(c.ttl)
	if exp.After(now) {
		return e.Value, Hit
	}

	exp = e.Born.Add(c.maxTTL)
	if exp.After(now) {
		return e.Value, GonnaMissy
	}

	c.lock.Lock()
	delete(c.tickets, clientID)
	c.lock.Unlock()
	return "", Miss
}

func (c *Cache) LoadByAlias(alias string) (string, Status) {
	c.lock.RLock()
	clientID, ok := c.aliases[alias]
	c.lock.RUnlock()
	if !ok {
		return "", Miss
	}

	return c.Load(clientID)
}

func (c *Cache) Store(clientID tvm.ClientID, alias string, entry Entry) {
	if time.Now().After(entry.Born.Add(c.maxTTL)) {
		// skip already missed entries
		return
	}

	c.lock.Lock()
	c.aliases[alias] = clientID
	c.tickets[clientID] = entry
	c.lock.Unlock()
}
