package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/logic"
	"mychat/internal/logic/kafkapush"
	"mychat/internal/logic/messagestore"
	"mychat/internal/logic/routing"
	"mychat/internal/model"
	"mychat/internal/service/gormservice"
)

var processMu sync.Mutex
var processActive bool

// Run serves HTTP/MCP and authenticated unary RPC. Database schema must already
// exist. Redis/Kafka failures affect readiness and operations, not HTTP startup.
func Run(ctx context.Context, c Config) error {
	if err := validate(c); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	processMu.Lock()
	if processActive || dao.GormDB != nil {
		processMu.Unlock()
		return errors.New("Logic cannot replace an installed process database")
	}
	processActive = true
	processMu.Unlock()
	defer func() { processMu.Lock(); processActive = false; processMu.Unlock() }()
	db, raw, err := openDatabase(ctx, c.Base.MySQLConfig)
	if err != nil {
		return err
	}
	defer raw.Close()
	if err := verifySchema(ctx, db); err != nil {
		return err
	}
	rc := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(c.Base.RedisConfig.Host, strconv.Itoa(c.Base.RedisConfig.Port)), Password: c.Base.RedisConfig.Password, DB: c.Base.RedisConfig.DB, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer rc.Close()
	routes, err := routing.New(rc, c.RedisPrefix, time.Minute, time.Minute)
	if err != nil {
		return errConfig
	}
	publisher, err := kafkapush.New(c.Base.KafkaConfig.Brokers, c.Base.KafkaConfig.Topic)
	if err != nil {
		return errConfig
	}
	defer publisher.Close()
	service, err := logic.NewService(context.Background(), logic.Config{Routes: routes, Messages: messagestore.New(db), Publisher: publisher, JWTSecret: c.Base.JWTConfig.Secret, JobToken: c.JobToken, KickToken: c.KickToken, Comets: c.Comets, UserExists: func(ctx context.Context, id string) (bool, error) {
		var count int64
		err := db.WithContext(ctx).Model(&model.UserInfo{}).Where("uuid = ?", id).Count(&count).Error
		if err != nil {
			return false, errDatabase
		}
		return count == 1, nil
	}})
	if err != nil {
		return errConfig
	}
	defer service.Close()
	httpListener, err := net.Listen("tcp", c.HTTPAddress)
	if err != nil {
		return errors.New("Logic HTTP listener unavailable")
	}
	defer httpListener.Close()
	rpcListener, err := net.Listen("tcp", c.RPCAddress)
	if err != nil {
		return errors.New("Logic RPC listener unavailable")
	}
	defer rpcListener.Close()
	probe := func(ctx context.Context) logic.DependencyStatus {
		var state logic.DependencyStatus
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); state.Database = raw.PingContext(ctx) == nil }()
		go func() { defer wg.Done(); state.Routes = routes.Ping(ctx) == nil }()
		go func() { defer wg.Done(); state.Push = publisher.Ping(ctx) == nil }()
		wg.Wait()
		return state
	}
	oldRedis := dao.RedisClient
	dao.GormDB = db
	dao.RedisClient = nil
	gormservice.InitJWT(c.Base.JWTConfig)
	defer func() { dao.GormDB = nil; dao.RedisClient = oldRedis; gormservice.InitJWT(config.JWTConfig{}) }()
	server := &http.Server{Handler: logic.NewHandler(service, db, probe), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	rpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(256 * 1024))
	pb.RegisterLogicServiceServer(rpcServer, service)
	// Defer order: drain both servers, then stop workers before clients/pool close.
	return serve(ctx, server, rpcServer, httpListener, rpcListener)
}

func serve(ctx context.Context, httpServer *http.Server, rpcServer *grpc.Server, httpListener, rpcListener net.Listener) error {
	results := make(chan error, 2)
	go func() { results <- httpServer.Serve(httpListener) }()
	go func() { results <- rpcServer.Serve(rpcListener) }()
	var result error
	select {
	case <-ctx.Done():
	case err := <-results:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			result = errors.New("Logic server stopped unexpectedly")
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpDone := make(chan error, 1)
	rpcDone := make(chan struct{})
	go func() { httpDone <- httpServer.Shutdown(shutdown) }()
	go func() { rpcServer.GracefulStop(); close(rpcDone) }()
	select {
	case <-rpcDone:
	case <-shutdown.Done():
		rpcServer.Stop()
		<-rpcDone
		if result == nil {
			result = errors.New("Logic shutdown exceeded grace period")
		}
	}
	if err := <-httpDone; err != nil {
		httpServer.Close()
		if result == nil {
			result = errors.New("Logic shutdown exceeded grace period")
		}
	}
	return result
}
