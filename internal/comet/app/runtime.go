package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/comet"
)

// Run starts internal RPC before registering the instance with Logic. A single
// reusable gRPC ClientConn pools calls; it is closed after all socket handlers
// and instance cleanup finish. A failed lease permanently stops this boot.
func Run(ctx context.Context, c Config) error {
	if e := validate(c); e != nil {
		return e
	}
	if ctx.Err() != nil {
		return nil
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	hub, e := comet.NewHub(&pb.CometIdentity{InstanceId: c.InstanceID, BootId: c.BootID}, c.QueueSize)
	if e != nil {
		return errConfig
	}
	defer hub.Close()
	service, e := comet.NewRPCService(hub, comet.RPCCredentials{JobToken: c.JobToken, LogicToken: c.KickToken, AdminToken: c.AdminToken})
	if e != nil {
		return errConfig
	}
	pool, e := grpc.NewClient(c.LogicAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256*1024), grpc.MaxCallSendMsgSize(256*1024)))
	if e != nil {
		return errors.New("Comet Logic pool unavailable")
	}
	defer pool.Close()
	rpcListener, e := net.Listen("tcp", c.RPCAddress)
	if e != nil {
		return errors.New("Comet RPC listener unavailable")
	}
	defer rpcListener.Close()
	rpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(256*1024), grpc.MaxSendMsgSize(256*1024))
	pb.RegisterCometServiceServer(rpcServer, service)
	results := make(chan error, 2)
	go func() { results <- rpcServer.Serve(rpcListener) }()
	defer rpcServer.Stop()
	instance, e := comet.StartInstance(lifetime, hub, pb.NewLogicServiceClient(pool), c.RPCAddress, c.LogicToken, comet.InstanceOptions{})
	if e != nil {
		return errors.New("Comet instance registration failed")
	}
	defer instance.Close()
	handler, e := comet.NewHandler(hub, pb.NewLogicServiceClient(pool), comet.HandlerConfig{JWTSecret: []byte(c.JWTSecret), LogicToken: c.LogicToken, AllowedOrigins: c.AllowedOrigins, Ready: instance.Ready})
	if e != nil {
		return errConfig
	}
	httpListener, e := net.Listen("tcp", c.HTTPAddress)
	if e != nil {
		return errors.New("Comet HTTP listener unavailable")
	}
	defer httpListener.Close()
	// net/http Shutdown does not wait for hijacked websockets. Track handlers
	// explicitly, close the Hub first, then wait before closing the shared pool.
	var handlers sync.WaitGroup
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		handler.ServeHTTP(w, r)
	}), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return lifetime }}
	go func() { results <- server.Serve(httpListener) }()
	var result error
	select {
	case <-ctx.Done():
	case <-instance.Done():
		result = errors.New("Comet instance lease stopped")
	case <-results:
		result = errors.New("Comet server stopped unexpectedly")
	}
	cancel()
	hub.Close()
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Shutdown(shutdown) }()
	rpcDone := make(chan struct{})
	go func() { rpcServer.GracefulStop(); close(rpcDone) }()
	select {
	case <-rpcDone:
	case <-shutdown.Done():
		rpcServer.Stop()
		<-rpcDone
	}
	if e := <-httpDone; e != nil {
		server.Close()
		if result == nil {
			result = errors.New("Comet shutdown exceeded grace period")
		}
	}
	handlers.Wait()
	instance.Close()
	return result
}
