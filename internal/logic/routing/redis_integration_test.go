//go:build integration

package routing

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "mychat/api/gen/fable/v1"
)

// Each run owns its namespace and deletes only keys in that namespace. Never
// flush a database: this suite can safely share a server with other test suites.
func realStore(t *testing.T, instanceTTL, routeTTL time.Duration) (*Store, *redis.Client) {
	t.Helper()
	address := os.Getenv("FABLE_LOGIC_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("set FABLE_LOGIC_TEST_REDIS_ADDR for real Redis lease tests")
	}
	c := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Fatalf("Redis: %v", err)
	}
	prefix := "logic-test:" + uuid.NewString() + ":"
	s, err := New(c, prefix, instanceTTL, routeTTL)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, err := c.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				t.Errorf("namespace cleanup: %v", err)
				break
			}
			if len(keys) > 0 {
				if err = c.Del(ctx, keys...).Err(); err != nil {
					t.Errorf("namespace delete: %v", err)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		c.Close()
	})
	return s, c
}
func TestRealRedisServerTimeAuthCapsAndExpiration(t *testing.T) {
	s, c := realStore(t, 2*time.Second, time.Second)
	id := comet()
	if _, err := s.Register(ctx, id, "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	now, err := c.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	// This expiry need not use the process clock. Lua compares it to Redis TIME.
	expiry := now.Add(6 * time.Second)
	r, _, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, expiry)
	if err != nil {
		t.Fatal(err)
	}
	ttl, err := c.PTTL(ctx, s.routes+user1).Result()
	if err != nil || ttl <= 0 || ttl > time.Second {
		t.Fatalf("route TTL: %v %v", ttl, err)
	}
	if r.LeaseDeadline.AsTime().After(expiry) || r.LeaseDeadline.AsTime().Before(now.Add(900*time.Millisecond)) {
		t.Fatalf("deadline: %v", r)
	}
	// A renewed route can never outlive the auth expiry even with a longer TTL.
	longer, err := New(c, "unused:", time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	longer.routes = s.routes
	longer.instances = s.instances
	renewed, err := longer.Heartbeat(ctx, r.Identity)
	if err != nil || !renewed.LeaseDeadline.AsTime().Equal(expiry.Truncate(time.Millisecond)) {
		t.Fatalf("auth cap: %v %v", renewed, err)
	}
	if _, _, err = s.Connect(ctx, id, user2, uuid.NewString(), capability, now.Add(5*time.Second)); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("unsafe token: %v", err)
	}
	// Independently expire only the instance; a still-stored route becomes invalid.
	if err = c.PExpire(ctx, s.instances+id.InstanceId, time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		exists, err := c.Exists(ctx, s.instances+id.InstanceId).Result()
		if err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("instance did not expire")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err = s.Check(ctx, r.Identity); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("expired instance accepted: %v", err)
	}
	if _, err = s.Renew(ctx, id, false); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("expired instance renewed: %v", err)
	}
}
func TestRealRedisConcurrentRouteFencesAndMonotonicDrain(t *testing.T) {
	s, _ := realStore(t, time.Minute, time.Minute)
	id := comet()
	if _, err := s.Register(ctx, id, "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	routes := make(chan *pb.RouteLease, 32)
	for range 32 {
		wg.Go(func() {
			r, _, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, time.Now().Add(time.Hour))
			if err != nil {
				t.Error(err)
				return
			}
			routes <- r
		})
	}
	wg.Wait()
	close(routes)
	found, err := s.Lookup(ctx, []string{user2, user1})
	if err != nil || len(found) != 2 || found[0].GetOffline() != pb.OfflineReason_OFFLINE_REASON_NO_ROUTE || found[1].GetRoute() == nil {
		t.Fatalf("ordered lookup: %v %v", found, err)
	}
	winner := found[1].GetRoute()
	for old := range routes {
		if old.Identity.ConnectionId == winner.Identity.ConnectionId {
			continue
		}
		wg.Go(func() {
			if _, err := s.Heartbeat(ctx, old.Identity); !errors.Is(err, ErrSuperseded) {
				t.Errorf("old heartbeat: %v", err)
			}
			if outcome, err := s.Disconnect(ctx, old.Identity); err != nil || outcome != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
				t.Errorf("old disconnect: %v %v", outcome, err)
			}
		})
	}
	wg.Wait()
	if _, err = s.Check(ctx, winner.Identity); err != nil {
		t.Fatal(err)
	}
	for n := range 16 {
		wg.Go(func() {
			if _, err := s.Renew(ctx, id, n == 0); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	lease, err := s.Register(ctx, id, "127.0.0.1:9000")
	if err != nil || !lease.Draining {
		t.Fatalf("draining reset: %v %v", lease, err)
	}
	if _, _, err = s.Connect(ctx, id, user2, uuid.NewString(), capability, time.Now().Add(time.Hour)); !errors.Is(err, ErrDraining) {
		t.Fatalf("draining connect: %v", err)
	}
	if _, err = s.Check(ctx, winner.Identity); err != nil {
		t.Fatalf("existing draining route: %v", err)
	}
	if _, err = s.Unregister(ctx, id); err != nil {
		t.Fatal(err)
	}
	newer := comet()
	if _, err = s.Register(ctx, newer, "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Heartbeat(ctx, winner.Identity); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("reboot resurrected route: %v", err)
	}
	if outcome, err := s.Unregister(ctx, id); err != nil || outcome != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
		t.Fatalf("old unregister: %v %v", outcome, err)
	}
}

func TestRealRedisPreservesFarFutureAuthExpiry(t *testing.T) {
	s, _ := realStore(t, time.Minute, time.Minute)
	id := comet()
	if _, err := s.Register(ctx, id, "localhost:9000"); err != nil {
		t.Fatal(err)
	}
	expires := time.Date(9999, 1, 1, 0, 0, 0, 987000000, time.UTC)
	r, _, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, expires)
	if err != nil || !r.AuthExpiresAt.AsTime().Equal(expires) {
		t.Fatalf("auth expiry rounded: %v %v", r, err)
	}
	checked, err := s.Check(ctx, r.Identity)
	if err != nil || !checked.AuthExpiresAt.AsTime().Equal(expires) {
		t.Fatalf("stored auth expiry rounded: %v %v", checked, err)
	}
}
