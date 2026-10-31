// Package routing owns Logic's Redis instance and connection leases.
package routing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "mychat/api/gen/fable/v1"
)

var (
	ErrInvalid       = errors.New("invalid routing argument")
	ErrConflict      = errors.New("routing registration conflict")
	ErrNotRegistered = errors.New("comet not registered")
	ErrSuperseded    = errors.New("connection superseded")
	ErrLeaseExpired  = errors.New("routing lease expired")
	ErrDraining      = errors.New("comet draining")
	ErrDependency    = errors.New("routing dependency unavailable")
)

// Store borrows its client; the caller owns closing it. All instances and routes
// in a prefix share one Redis Cluster slot to preserve cross-key identity fences.
type Store struct {
	client                redis.UniversalClient
	instances, routes     string
	instanceTTL, routeTTL int64
}

// New defaults zero TTLs to 60 seconds. Positive TTLs are rounded up to Redis's
// millisecond resolution without overflowing time.Duration arithmetic.
func New(client redis.UniversalClient, prefix string, instanceTTL, routeTTL time.Duration) (*Store, error) {
	if client == nil || (reflect.ValueOf(client).Kind() == reflect.Ptr && reflect.ValueOf(client).IsNil()) || instanceTTL < 0 || routeTTL < 0 || len(prefix) > 256 || strings.ContainsAny(prefix, "{}\r\n\x00") {
		return nil, ErrInvalid
	}
	ttl := func(d time.Duration) int64 {
		if d == 0 {
			d = time.Minute
		}
		return int64((d-1)/time.Millisecond) + 1
	}
	base := prefix + "{logic-routing}:"
	return &Store{client: client, instances: base + "instance:", routes: base + "route:", instanceTTL: ttl(instanceTTL), routeTTL: ttl(routeTTL)}, nil
}

type record struct {
	User       string `json:"user,omitempty"`
	Instance   string `json:"instance,omitempty"`
	Boot       string `json:"boot,omitempty"`
	Connection string `json:"connection,omitempty"`
	Token      string `json:"token,omitempty"`
	Address    string `json:"address,omitempty"`
	Draining   bool   `json:"draining"`
	// Decimal strings preserve the full Timestamp range through Redis cjson.
	Auth     int64    `json:"auth,omitempty,string"`
	Deadline int64    `json:"deadline,omitempty"`
	IDs      []string `json:"ids,omitempty"`
}
type result struct {
	Err      string                      `json:"err"`
	Instance *record                     `json:"instance"`
	Route    *record                     `json:"route"`
	Previous *record                     `json:"previous"`
	Outcome  pb.ConditionalDeleteOutcome `json:"outcome"`
	Results  []struct {
		ID       string  `json:"id"`
		Reason   int32   `json:"reason"`
		Route    *record `json:"route"`
		Instance *record `json:"instance"`
	} `json:"results"`
}

func (s *Store) run(ctx context.Context, op string, p record, keys ...string) (*result, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, ErrInvalid
	}
	raw, err := leaseScript.Run(ctx, s.client, keys, op, string(b), s.instances, s.routes, s.instanceTTL, s.routeTTL).Text()
	if err != nil {
		return nil, dependency(err)
	}
	var r result
	if err = json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, ErrDependency
	}
	switch r.Err {
	case "":
		return &r, nil
	case "conflict":
		return nil, ErrConflict
	case "not_registered":
		return nil, ErrNotRegistered
	case "superseded":
		return nil, ErrSuperseded
	case "expired":
		return nil, ErrLeaseExpired
	case "draining":
		return nil, ErrDraining
	case "invalid":
		return nil, ErrInvalid
	default:
		return nil, ErrDependency
	}
}

// Dependency errors deliberately omit Redis error text, which may contain keys
// or credentials. Context cancellation remains discoverable with errors.Is.
func dependency(err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", ErrDependency, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrDependency, context.DeadlineExceeded)
	}
	return ErrDependency
}
func validUUID(s string) bool {
	return len(s) == 36 && s != uuid.Nil.String() && uuid.Validate(s) == nil
}
func validUser(s string) bool { return len(s) == 37 && s[0] == 'U' && validUUID(s[1:]) }
func validInstance(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func validComet(id *pb.CometIdentity) bool {
	return id != nil && validInstance(id.InstanceId) && validUUID(id.BootId)
}
func validConnection(id *pb.ConnectionIdentity) bool {
	if id == nil || !validUser(id.UserId) || !validInstance(id.InstanceId) || !validUUID(id.ConnectionId) || len(id.RouteToken) > 128 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(id.RouteToken)
	return err == nil && len(raw) >= 16 && base64.RawURLEncoding.EncodeToString(raw) == id.RouteToken
}
func validAddress(address string) bool {
	if len(address) > 512 || strings.ContainsAny(address, "/@?#\r\n\t ") {
		return false
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535 && strconv.Itoa(n) == port
}
func connectionRecord(id *pb.ConnectionIdentity) record {
	return record{User: id.UserId, Instance: id.InstanceId, Connection: id.ConnectionId, Token: id.RouteToken}
}
func cometLease(r *record) *pb.CometLease {
	if r == nil {
		return nil
	}
	return &pb.CometLease{Identity: &pb.CometIdentity{InstanceId: r.Instance, BootId: r.Boot}, RpcAddress: r.Address, Draining: r.Draining, LeaseDeadline: timestamppb.New(time.UnixMilli(r.Deadline))}
}
func routeLease(r *record) *pb.RouteLease {
	if r == nil {
		return nil
	}
	return &pb.RouteLease{Identity: &pb.ConnectionIdentity{UserId: r.User, InstanceId: r.Instance, ConnectionId: r.Connection, RouteToken: r.Token}, AuthExpiresAt: timestamppb.New(time.UnixMilli(r.Auth)), LeaseDeadline: timestamppb.New(time.UnixMilli(r.Deadline))}
}
func (s *Store) Register(ctx context.Context, id *pb.CometIdentity, address string) (*pb.CometLease, error) {
	if !validComet(id) || !validAddress(address) {
		return nil, ErrInvalid
	}
	r, err := s.run(ctx, "register", record{Instance: id.InstanceId, Boot: id.BootId, Address: address}, s.instances+id.InstanceId)
	if err != nil {
		return nil, err
	}
	return cometLease(r.Instance), nil
}
func (s *Store) Renew(ctx context.Context, id *pb.CometIdentity, draining bool) (*pb.CometLease, error) {
	if !validComet(id) {
		return nil, ErrInvalid
	}
	r, err := s.run(ctx, "renew", record{Instance: id.InstanceId, Boot: id.BootId, Draining: draining}, s.instances+id.InstanceId)
	if err != nil {
		return nil, err
	}
	return cometLease(r.Instance), nil
}
func (s *Store) Unregister(ctx context.Context, id *pb.CometIdentity) (pb.ConditionalDeleteOutcome, error) {
	if !validComet(id) {
		return 0, ErrInvalid
	}
	r, err := s.run(ctx, "unregister", record{Instance: id.InstanceId, Boot: id.BootId}, s.instances+id.InstanceId)
	if err != nil {
		return 0, err
	}
	return r.Outcome, nil
}
func (s *Store) Connect(ctx context.Context, id *pb.CometIdentity, user, connectionID, token string, authExpires time.Time) (*pb.RouteLease, *pb.RouteLease, error) {
	identity := &pb.ConnectionIdentity{UserId: user, ConnectionId: connectionID, RouteToken: token}
	if id != nil {
		identity.InstanceId = id.InstanceId
	}
	if !validComet(id) || !validConnection(identity) || authExpires.IsZero() || timestamppb.New(authExpires).CheckValid() != nil {
		return nil, nil, ErrInvalid
	}
	p := connectionRecord(identity)
	p.Boot = id.BootId
	p.Auth = authExpires.UnixMilli()
	r, err := s.run(ctx, "connect", p, s.routes+user, s.instances+id.InstanceId)
	if err != nil {
		return nil, nil, err
	}
	return routeLease(r.Route), routeLease(r.Previous), nil
}
func (s *Store) route(ctx context.Context, op string, id *pb.ConnectionIdentity) (*pb.RouteLease, error) {
	if !validConnection(id) {
		return nil, ErrInvalid
	}
	r, err := s.run(ctx, op, connectionRecord(id), s.routes+id.UserId)
	if err != nil {
		return nil, err
	}
	return routeLease(r.Route), nil
}
func (s *Store) Heartbeat(ctx context.Context, id *pb.ConnectionIdentity) (*pb.RouteLease, error) {
	return s.route(ctx, "heartbeat", id)
}
func (s *Store) Check(ctx context.Context, id *pb.ConnectionIdentity) (*pb.RouteLease, error) {
	return s.route(ctx, "check", id)
}
func (s *Store) Disconnect(ctx context.Context, id *pb.ConnectionIdentity) (pb.ConditionalDeleteOutcome, error) {
	if !validConnection(id) {
		return 0, ErrInvalid
	}
	r, err := s.run(ctx, "disconnect", connectionRecord(id), s.routes+id.UserId)
	if err != nil {
		return 0, err
	}
	return r.Outcome, nil
}
func validBatch(ids []string, valid func(string) bool) bool {
	if len(ids) < 1 || len(ids) > 100 {
		return false
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !valid(id) || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
func (s *Store) Lookup(ctx context.Context, ids []string) ([]*pb.RouteLookupResult, error) {
	if !validBatch(ids, validUser) {
		return nil, ErrInvalid
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.routes + id
	}
	r, err := s.run(ctx, "lookup", record{IDs: ids}, keys...)
	if err != nil {
		return nil, err
	}
	if len(r.Results) != len(ids) {
		return nil, ErrDependency
	}
	out := make([]*pb.RouteLookupResult, len(ids))
	for i, item := range r.Results {
		v := &pb.RouteLookupResult{UserId: item.ID}
		if item.Route != nil {
			v.State = &pb.RouteLookupResult_Route{Route: routeLease(item.Route)}
		} else if item.Reason >= 1 && item.Reason <= 3 {
			v.State = &pb.RouteLookupResult_Offline{Offline: pb.OfflineReason(item.Reason)}
		} else {
			return nil, ErrDependency
		}
		out[i] = v
	}
	return out, nil
}
func (s *Store) Resolve(ctx context.Context, ids []string) ([]*pb.CometResolution, error) {
	if !validBatch(ids, validInstance) {
		return nil, ErrInvalid
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.instances + id
	}
	r, err := s.run(ctx, "resolve", record{IDs: ids}, keys...)
	if err != nil {
		return nil, err
	}
	if len(r.Results) != len(ids) {
		return nil, ErrDependency
	}
	out := make([]*pb.CometResolution, len(ids))
	for i, item := range r.Results {
		v := &pb.CometResolution{InstanceId: item.ID}
		if item.Instance != nil {
			v.State = &pb.CometResolution_Lease{Lease: cometLease(item.Instance)}
		} else if item.Reason >= 1 && item.Reason <= 2 {
			v.State = &pb.CometResolution_Missing{Missing: pb.MissingCometReason(item.Reason)}
		} else {
			return nil, ErrDependency
		}
		out[i] = v
	}
	return out, nil
}
func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return dependency(err)
	}
	return nil
}
