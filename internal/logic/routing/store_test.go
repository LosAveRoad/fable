package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "mychat/api/gen/fable/v1"
)

var ctx = context.Background()

const user1 = "U11111111-1111-4111-8111-111111111111"
const user2 = "U22222222-2222-4222-8222-222222222222"
const capability = "MDEyMzQ1Njc4OWFiY2RlZg"

func comet() *pb.CometIdentity {
	return &pb.CometIdentity{InstanceId: "comet-a", BootId: uuid.NewString()}
}
func setup(t *testing.T) (*Store, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	m := miniredis.RunT(t)
	m.SetTime(time.Now())
	c := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
	t.Cleanup(func() { c.Close() })
	s, err := New(c, "test:"+uuid.NewString()+":", time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return s, m, c
}
func connect(t *testing.T, s *Store, c *pb.CometIdentity, user string) *pb.RouteLease {
	t.Helper()
	r, _, err := s.Connect(ctx, c, user, uuid.NewString(), capability, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestRegisterFencesBootAddressAndPreservesDraining(t *testing.T) {
	s, _, _ := setup(t)
	id := comet()
	a, err := s.Register(ctx, id, "127.0.0.1:9000")
	if err != nil || a.Draining {
		t.Fatalf("register: %v %v", a, err)
	}
	if _, err = s.Register(ctx, id, "127.0.0.1:9001"); !errors.Is(err, ErrConflict) {
		t.Fatalf("address conflict: %v", err)
	}
	other := comet()
	if _, err = s.Register(ctx, other, "127.0.0.1:9000"); !errors.Is(err, ErrConflict) {
		t.Fatalf("boot conflict: %v", err)
	}
	if _, err = s.Renew(ctx, other, false); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("old renew: %v", err)
	}
	if _, err = s.Renew(ctx, id, true); err != nil {
		t.Fatal(err)
	}
	for _, op := range []func() (*pb.CometLease, error){func() (*pb.CometLease, error) { return s.Renew(ctx, id, false) }, func() (*pb.CometLease, error) { return s.Register(ctx, id, "127.0.0.1:9000") }} {
		r, err := op()
		if err != nil || !r.Draining {
			t.Fatalf("drain reverted: %v %v", r, err)
		}
	}
	if out, err := s.Unregister(ctx, other); err != nil || out != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
		t.Fatalf("old unregister: %v %v", out, err)
	}
}
func TestConnectReplacementFencesOldHeartbeatAndDelete(t *testing.T) {
	s, _, _ := setup(t)
	id := comet()
	if _, err := s.Register(ctx, id, "localhost:9000"); err != nil {
		t.Fatal(err)
	}
	old := connect(t, s, id, user1)
	next, replaced, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, time.Now().Add(time.Hour))
	if err != nil || replaced == nil || replaced.Identity.ConnectionId != old.Identity.ConnectionId {
		t.Fatalf("replacement: %v %v", replaced, err)
	}
	if _, err = s.Heartbeat(ctx, old.Identity); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("old heartbeat: %v", err)
	}
	if _, err = s.Check(ctx, old.Identity); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("old check: %v", err)
	}
	if out, err := s.Disconnect(ctx, old.Identity); err != nil || out != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
		t.Fatalf("old delete: %v %v", out, err)
	}
	if _, err = s.Check(ctx, next.Identity); err != nil {
		t.Fatal(err)
	}
	if out, err := s.Disconnect(ctx, next.Identity); err != nil || out != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_DELETED {
		t.Fatalf("delete: %v %v", out, err)
	}
	if out, err := s.Disconnect(ctx, next.Identity); err != nil || out != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_FOUND {
		t.Fatalf("repeat delete: %v %v", out, err)
	}
}
func TestAuthCapAndRedisClock(t *testing.T) {
	s, m, _ := setup(t)
	now := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	m.SetTime(now)
	id := comet()
	s.Register(ctx, id, "localhost:9000")
	expires := now.Add(12 * time.Second)
	r, _, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, expires)
	if err != nil || !r.LeaseDeadline.AsTime().Equal(expires) {
		t.Fatalf("auth cap %v %v", r, err)
	}
	m.SetTime(now.Add(8 * time.Second))
	r, err = s.Heartbeat(ctx, r.Identity)
	if err != nil || !r.LeaseDeadline.AsTime().Equal(expires) {
		t.Fatalf("heartbeat cap %v %v", r, err)
	}
	m.SetTime(expires)
	if _, err = s.Check(ctx, r.Identity); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired check: %v", err)
	}
	for _, delta := range []time.Duration{-time.Second, 5 * time.Second} {
		if _, _, err = s.Connect(ctx, id, user1, uuid.NewString(), capability, expires.Add(delta)); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("unsafe JWT: %v", err)
		}
	}
}
func TestDrainingAllowsExistingRouteAndRejectsConnect(t *testing.T) {
	s, _, _ := setup(t)
	id := comet()
	s.Register(ctx, id, "localhost:9000")
	r := connect(t, s, id, user1)
	s.Renew(ctx, id, true)
	if _, _, err := s.Connect(ctx, id, user2, uuid.NewString(), capability, time.Now().Add(time.Hour)); !errors.Is(err, ErrDraining) {
		t.Fatalf("connect draining: %v", err)
	}
	if _, err := s.Check(ctx, r.Identity); err != nil {
		t.Fatalf("existing check: %v", err)
	}
	if _, err := s.Heartbeat(ctx, r.Identity); err != nil {
		t.Fatalf("existing heartbeat: %v", err)
	}
}
func TestInstanceExpiryRebootAndUnregisterNeverReviveRoute(t *testing.T) {
	s, m, _ := setup(t)
	id := comet()
	s.Register(ctx, id, "localhost:9000")
	r := connect(t, s, id, user1)
	if _, err := s.Unregister(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Check(ctx, r.Identity); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("unregistered check: %v", err)
	}
	newer := comet()
	s.Register(ctx, newer, "localhost:9000")
	if _, err := s.Heartbeat(ctx, r.Identity); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("reboot revived route: %v", err)
	}
	m.FastForward(61 * time.Second)
	if _, err := s.Renew(ctx, newer, false); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("expired renew: %v", err)
	}
	if _, err := s.Register(ctx, id, "localhost:9000"); err != nil {
		t.Fatal(err)
	}
}
func TestOrderedLookupResolveAndDependencyFailure(t *testing.T) {
	s, m, _ := setup(t)
	id := comet()
	s.Register(ctx, id, "localhost:9000")
	r := connect(t, s, id, user1)
	results, err := s.Lookup(ctx, []string{user2, user1})
	if err != nil || len(results) != 2 || results[0].UserId != user2 || results[0].GetOffline() != pb.OfflineReason_OFFLINE_REASON_NO_ROUTE || results[1].GetRoute().Identity.ConnectionId != r.Identity.ConnectionId {
		t.Fatalf("lookup: %v %v", results, err)
	}
	s.Renew(ctx, id, true)
	resolved, err := s.Resolve(ctx, []string{"absent", id.InstanceId})
	if err != nil || len(resolved) != 2 || resolved[0].GetMissing() != pb.MissingCometReason_MISSING_COMET_REASON_NOT_REGISTERED || !resolved[1].GetLease().Draining {
		t.Fatalf("resolve: %v %v", resolved, err)
	}
	s.Unregister(ctx, id)
	results, err = s.Lookup(ctx, []string{user1})
	if err != nil || results[0].GetOffline() != pb.OfflineReason_OFFLINE_REASON_INSTANCE_EXPIRED {
		t.Fatalf("invalid instance route: %v %v", results, err)
	}
	m.Close()
	if _, err = s.Lookup(ctx, []string{user1}); !errors.Is(err, ErrDependency) {
		t.Fatalf("dependency became offline: %v", err)
	}
	if err = s.Ping(ctx); !errors.Is(err, ErrDependency) {
		t.Fatalf("ping: %v", err)
	}
}
func TestConcurrentReplacementCannotDeleteWinner(t *testing.T) {
	s, _, _ := setup(t)
	id := comet()
	s.Register(ctx, id, "localhost:9000")
	var wg sync.WaitGroup
	ids := make(chan *pb.ConnectionIdentity, 20)
	for range 20 {
		wg.Go(func() {
			r, _, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, time.Now().Add(time.Hour))
			if err != nil {
				t.Error(err)
				return
			}
			ids <- r.Identity
		})
	}
	wg.Wait()
	close(ids)
	results, err := s.Lookup(ctx, []string{user1})
	if err != nil {
		t.Fatal(err)
	}
	winner := results[0].GetRoute().Identity
	for old := range ids {
		if old.ConnectionId != winner.ConnectionId {
			if _, err := s.Disconnect(ctx, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = s.Check(ctx, winner); err != nil {
		t.Fatalf("winner lost: %v", err)
	}
}
func TestValidationRejectsMalformedIdentitiesBatchesAndTTLs(t *testing.T) {
	s, _, c := setup(t)
	for _, ttl := range []time.Duration{-time.Second} {
		if _, err := New(c, "test", ttl, time.Minute); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad TTL %v: %v", ttl, err)
		}
	}
	for _, id := range []*pb.CometIdentity{nil, {}, {InstanceId: "Upper", BootId: uuid.NewString()}, {InstanceId: "a", BootId: "x"}} {
		if _, err := s.Register(ctx, id, "localhost:9000"); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad instance: %v", err)
		}
	}
	id := comet()
	for _, addr := range []string{"", "http://localhost:9000", "localhost:0", "localhost:99999", "localhost"} {
		if _, err := s.Register(ctx, id, addr); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad address %q: %v", addr, err)
		}
	}
	s.Register(ctx, id, "localhost:9000")
	for _, token := range []string{"short", capability + "=", "MDEyMzQ1Njc4OWFiY2RlZh"} {
		if _, _, err := s.Connect(ctx, id, user1, uuid.NewString(), token, time.Now().Add(time.Hour)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad token: %v", err)
		}
	}
	for _, ids := range [][]string{nil, {user1, user1}, {"invalid"}} {
		if _, err := s.Lookup(ctx, ids); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad lookup: %v", err)
		}
	}
	for _, ids := range [][]string{nil, {"a", "a"}, {"Upper"}} {
		if _, err := s.Resolve(ctx, ids); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad resolve: %v", err)
		}
	}
	for _, identity := range []*pb.ConnectionIdentity{nil, {}, {UserId: user1, InstanceId: "Upper", ConnectionId: uuid.NewString(), RouteToken: capability}} {
		if _, err := s.Check(ctx, identity); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad check: %v", err)
		}
	}
}

// Redis corruption must fail the request instead of exporting malformed hints
// or representing a dependency/data failure as a normal offline user.
func TestCorruptRedisLeaseFailsClosed(t *testing.T) {
	s, _, c := setup(t)
	id := comet()
	if _, err := s.Register(ctx, id, "localhost:9000"); err != nil {
		t.Fatal(err)
	}
	r := connect(t, s, id, user1)
	raw := `{"user":"not-a-user","instance":"comet-a","boot":"` + id.BootId + `","connection":"` + r.Identity.ConnectionId + `","token":"` + capability + `","auth":9999999999999,"deadline":9999999999999}`
	if err := c.Set(ctx, s.routes+user1, raw, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if results, err := s.Lookup(ctx, []string{user1}); !errors.Is(err, ErrDependency) || results != nil {
		t.Fatalf("corrupt route escaped: %v %v", results, err)
	}
	if _, _, err := s.Connect(ctx, id, user1, uuid.NewString(), capability, time.Now().Add(time.Hour)); !errors.Is(err, ErrDependency) {
		t.Fatalf("replacement silently accepted corrupt previous lease: %v", err)
	}
	raw = `{"instance":"comet-a","boot":"invalid-boot","address":"localhost:9000","draining":false,"deadline":9999999999999}`
	if err := c.Set(ctx, s.instances+id.InstanceId, raw, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if results, err := s.Resolve(ctx, []string{id.InstanceId}); !errors.Is(err, ErrDependency) || results != nil {
		t.Fatalf("corrupt instance escaped: %v %v", results, err)
	}
}

func TestPreservesAuthExpiryWithoutLuaJSONRounding(t *testing.T) {
	s, _, _ := setup(t)
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
