package job

import (
	"context"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (p *Processor) round(ctx context.Context, task im.PushTask, states []*recipient, repaired *bool) {
	lookup := []*recipient{}
	isRepair := false
	for _, s := range states {
		if s.Outcome == Unresolved && !s.stopped && s.route == nil {
			if s.repair && *repaired {
				s.stopped = true
				continue
			}
			isRepair = isRepair || s.repair
			lookup = append(lookup, s)
		}
	}
	if len(lookup) > 0 {
		if isRepair {
			*repaired = true
		}
		p.lookup(ctx, task.RequestID, lookup)
		for _, s := range lookup {
			if s.repair && s.route == nil {
				s.stopped = true
			}
		}
	}
	groups := map[string][]*recipient{}
	for _, s := range states {
		if s.Outcome == Unresolved && !s.stopped && s.route != nil {
			groups[s.route.InstanceId] = append(groups[s.route.InstanceId], s)
		}
	}
	instances := make([]string, 0, len(groups))
	for id := range groups {
		instances = append(instances, id)
	}
	sort.Strings(instances)
	if len(instances) == 0 || ctx.Err() != nil {
		return
	}
	rpcCtx, cancel := p.rpcContext(ctx)
	reply, err := p.logic.ResolveComets(rpcCtx, &pb.ResolveCometsRequest{InstanceIds: instances, RequestId: task.RequestID})
	cancel()
	if err != nil {
		for _, batch := range groups {
			rpcFailure(batch, err, false)
		}
		return
	}
	if !validResolutions(reply, instances) {
		for _, batch := range groups {
			mark(batch, InvalidResponse)
		}
		return
	}
	message, _ := messageView(task.Message)
	for i, id := range instances {
		if ctx.Err() != nil {
			break
		}
		batch := groups[id]
		resolution := reply.Results[i]
		lease := resolution.GetLease()
		if lease == nil {
			repair(batch)
			continue
		}
		rpcCtx, cancel := p.rpcContext(ctx)
		client, err := p.dial(rpcCtx, lease.RpcAddress)
		if err != nil || client == nil {
			cancel()
			mark(batch, DependencyFailure)
			continue
		}
		targets := make([]*pb.ConnectionIdentity, 0, len(batch))
		for _, s := range batch {
			targets = append(targets, s.route)
		}
		response, err := client.Deliver(rpcCtx, &pb.DeliverRequest{Comet: lease.Identity, TaskId: task.TaskID, Message: message, Targets: targets, RequestId: task.RequestID})
		cancel()
		if err != nil {
			rpcFailure(batch, err, true)
			continue
		}
		if !validDelivery(response, task.TaskID, message.MessageId, targets) {
			repairWithReason(batch, InvalidResponse)
			continue
		}
		for j, item := range response.Results {
			s := batch[j]
			switch item.Outcome {
			case pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED:
				s.Outcome = Enqueued
				s.Reason = ""
			case pb.DeliveryOutcome_DELIVERY_OUTCOME_DUPLICATE:
				s.Outcome = Duplicate
				s.Reason = ""
			default:
				repair([]*recipient{s})
			}
		}
	}
}

func (p *Processor) lookup(ctx context.Context, requestID string, states []*recipient) {
	ids := make([]string, 0, len(states))
	for _, s := range states {
		ids = append(ids, s.UserID)
	}
	rpcCtx, cancel := p.rpcContext(ctx)
	defer cancel()
	r, err := p.logic.LookupRoutes(rpcCtx, &pb.LookupRoutesRequest{UserIds: ids, RequestId: requestID})
	if err != nil {
		rpcFailure(states, err, false)
		return
	}
	if !validLookups(r, ids) {
		mark(states, InvalidResponse)
		return
	}
	for i, s := range states {
		item := r.Results[i]
		if route := item.GetRoute(); route != nil {
			s.route = route.Identity
			s.repair = false
		} else {
			s.Outcome = Offline
			s.Reason = ""
		}
	}
}

func mark(states []*recipient, reason Reason) {
	for _, s := range states {
		s.Reason = reason
	}
}
func repair(states []*recipient) {
	repairWithReason(states, RouteChanged)
}
func repairWithReason(states []*recipient, reason Reason) {
	for _, s := range states {
		s.route = nil
		s.repair = true
		s.Reason = reason
	}
}

func rpcFailure(states []*recipient, err error, refresh bool) {
	code := status.Code(err)
	permanent := code == codes.InvalidArgument || code == codes.PermissionDenied || code == codes.Unauthenticated || code == codes.AlreadyExists || code == codes.Unimplemented
	for _, item := range status.Convert(err).Details() {
		if detail, ok := item.(*pb.ErrorDetail); ok {
			switch detail.Retry {
			case pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY, pb.RetryDirective_RETRY_DIRECTIVE_REAUTHENTICATE:
				permanent = true
			case pb.RetryDirective_RETRY_DIRECTIVE_SAME_REQUEST:
				refresh = false
			case pb.RetryDirective_RETRY_DIRECTIVE_REFRESH_ROUTE:
				refresh = true
			}
		}
	}
	if permanent {
		for _, s := range states {
			s.Reason = DeliveryRejected
			s.stopped = true
		}
		return
	}
	if refresh {
		repairWithReason(states, DependencyFailure)
	} else {
		mark(states, DependencyFailure)
	}
}

func future(t *timestamppb.Timestamp) bool {
	return t != nil && t.CheckValid() == nil && t.AsTime().After(time.Now())
}
func validLookups(r *pb.LookupRoutesResponse, ids []string) bool {
	if r == nil || len(r.Results) != len(ids) {
		return false
	}
	for i, item := range r.Results {
		if item == nil || item.UserId != ids[i] {
			return false
		}
		switch v := item.State.(type) {
		case *pb.RouteLookupResult_Route:
			a := v.Route
			if a == nil || !validConnection(a.Identity) || a.Identity.UserId != ids[i] || !future(a.AuthExpiresAt) || !future(a.LeaseDeadline) || a.LeaseDeadline.AsTime().After(a.AuthExpiresAt.AsTime()) {
				return false
			}
		case *pb.RouteLookupResult_Offline:
			if v.Offline != pb.OfflineReason_OFFLINE_REASON_NO_ROUTE && v.Offline != pb.OfflineReason_OFFLINE_REASON_LEASE_EXPIRED && v.Offline != pb.OfflineReason_OFFLINE_REASON_INSTANCE_EXPIRED {
				return false
			}
		default:
			return false
		}
	}
	return true
}
func validResolutions(r *pb.ResolveCometsResponse, ids []string) bool {
	if r == nil || len(r.Results) != len(ids) {
		return false
	}
	for i, item := range r.Results {
		if item == nil || item.InstanceId != ids[i] {
			return false
		}
		switch v := item.State.(type) {
		case *pb.CometResolution_Lease:
			a := v.Lease
			if a == nil || a.Identity == nil || a.Identity.InstanceId != ids[i] || !validUUID(a.Identity.BootId) || !future(a.LeaseDeadline) || !validAddress(a.RpcAddress) {
				return false
			}
		case *pb.CometResolution_Missing:
			if v.Missing != pb.MissingCometReason_MISSING_COMET_REASON_NOT_REGISTERED && v.Missing != pb.MissingCometReason_MISSING_COMET_REASON_LEASE_EXPIRED {
				return false
			}
		default:
			return false
		}
	}
	return true
}
func validAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	n, e := strconv.Atoi(port)
	return err == nil && e == nil && n > 0 && n <= 65535 && host != "" && !strings.ContainsAny(host, "/@?# \t\r\n\\")
}
func sameTarget(a, b *pb.ConnectionIdentity) bool {
	return a != nil && b != nil && a.UserId == b.UserId && a.InstanceId == b.InstanceId && a.ConnectionId == b.ConnectionId && a.RouteToken == b.RouteToken
}
func validDelivery(r *pb.DeliverResponse, task, message string, targets []*pb.ConnectionIdentity) bool {
	if r == nil || r.TaskId != task || r.MessageId != message || len(r.Results) != len(targets) {
		return false
	}
	for i, item := range r.Results {
		if item == nil || !sameTarget(item.Target, targets[i]) || item.Outcome < pb.DeliveryOutcome_DELIVERY_OUTCOME_ENQUEUED || item.Outcome > pb.DeliveryOutcome_DELIVERY_OUTCOME_DRAINING {
			return false
		}
	}
	return true
}
