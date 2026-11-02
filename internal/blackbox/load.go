package blackbox

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"mychat/internal/dto/im"
)

// LoadConfig bounds the generator; Rate is the aggregate attempted sends/second.
// Pairs is the maximum number of simultaneous sends, each using distinct users.
type LoadConfig struct {
	Config
	Pairs           int           `json:"pairs"`
	Rate            float64       `json:"rate"`
	Duration        time.Duration `json:"duration"`
	ReceiveTimeout  time.Duration `json:"receive_timeout"`
	MaxSaveP95      time.Duration `json:"max_save_p95"`
	MaxReceiveP95   time.Duration `json:"max_receive_p95"`
	MinReceiveRatio float64       `json:"min_receive_ratio"`
}

func (c LoadConfig) normalized() (LoadConfig, error) {
	if c.Pairs == 0 {
		c.Pairs = 2
	}
	if c.Rate == 0 {
		c.Rate = 2
	}
	if c.Duration == 0 {
		c.Duration = 10 * time.Second
	}
	if c.ReceiveTimeout == 0 {
		c.ReceiveTimeout = 5 * time.Second
	}
	if c.Pairs < 1 || c.Pairs > 32 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate < 0.1 || c.Rate > 200 || c.Duration < time.Second || c.Duration > 5*time.Minute || c.ReceiveTimeout < 100*time.Millisecond || c.ReceiveTimeout > 30*time.Second || c.MaxSaveP95 < 0 || c.MaxReceiveP95 < 0 || math.IsNaN(c.MinReceiveRatio) || c.MinReceiveRatio < 0 || c.MinReceiveRatio > 1 {
		return c, errors.New("load configuration outside permitted bounds")
	}
	if c.Timeout == 0 {
		c.Timeout = c.Duration + 3*time.Minute
	}
	var e error
	c.Config, e = c.Config.normalized()
	return c, e
}

type Percentiles struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
}

func summarize(values []time.Duration) Percentiles {
	p := Percentiles{Samples: len(values)}
	if len(values) == 0 {
		return p
	}
	v := append([]time.Duration(nil), values...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	at := func(q float64) float64 {
		return float64(v[int(math.Ceil(q*float64(len(v))))-1]) / float64(time.Millisecond)
	}
	p.P50MS = at(.5)
	p.P95MS = at(.95)
	p.P99MS = at(.99)
	return p
}

type LoadReport struct {
	RunID              string      `json:"run_id"`
	Target             string      `json:"target"`
	Config             LoadConfig  `json:"config"`
	StartedAt          time.Time   `json:"started_at"`
	MeasuredDurationMS float64     `json:"measured_duration_ms"`
	Attempted          int         `json:"attempted"`
	Stored             int         `json:"stored"`
	Uncertain          int         `json:"uncertain"`
	Rejected           int         `json:"rejected"`
	Received           int         `json:"received_online"`
	Recovered          int         `json:"recovered_by_sync"`
	Duplicate          int         `json:"duplicate_pushes"`
	Missing            int         `json:"missing_after_repair"`
	GeneratorErrors    int         `json:"generator_errors"`
	RateLimitedTicks   int         `json:"generator_busy_ticks"`
	AchievedRate       float64     `json:"achieved_attempt_rate"`
	StoredRate         float64     `json:"stored_rate"`
	OnlineReceiveRatio float64     `json:"online_receive_ratio"`
	SaveLatency        Percentiles `json:"save_latency"`
	ReceiveLatency     Percentiles `json:"online_receive_latency"`
	Passed             bool        `json:"passed"`
	Error              string      `json:"error,omitempty"`
}
type loadPair struct {
	a, b        identity
	left, right *socket
	session     string
	turn        int
}
type loadAttempt struct {
	input    im.SendFrame
	receiver *socket
	start    time.Time
	save     time.Duration
	result   im.SendResultFrame
	err      error
	pair     int
}

// RunLoad warms up public fixtures before timing, sends bounded real messages,
// drains peer pushes, then repairs through authoritative HTTP history.
// Queue receipts count only as storage acknowledgments, never delivery.
func RunLoad(ctx context.Context, cfg LoadConfig) (report LoadReport, err error) {
	report.StartedAt = time.Now().UTC()
	cfg, err = cfg.normalized()
	if err != nil {
		report.Error = err.Error()
		return report, err
	}
	c, err := newClient(cfg.Config)
	if err != nil {
		report.Error = err.Error()
		return report, err
	}
	defer c.close()
	cfg.Config = c.config
	report.Config = cfg
	report.Target = cfg.BaseURL
	report.RunID = cfg.RunID
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	var pairs []loadPair
	defer func() {
		for _, p := range pairs {
			p.left.close()
			p.right.close()
			<-p.left.done
			<-p.right.done
		}
	}()
	fail := func(message string) (LoadReport, error) { report.Error = message; return report, errors.New(message) }
	for i := 0; i < cfg.Pairs; i++ {
		a, e := c.register(ctx)
		if e != nil {
			return fail("load fixture registration failed")
		}
		b, e := c.register(ctx)
		if e != nil {
			return fail("load fixture registration failed")
		}
		session, e := c.open(ctx, a, b)
		if e != nil {
			return fail("load session setup failed")
		}
		left, e := c.connect(ctx, a)
		if e != nil {
			return fail("load sender connect failed")
		}
		right, e := c.connect(ctx, b)
		if e != nil {
			left.close()
			<-left.done
			return fail("load receiver connect failed")
		}
		p := loadPair{a: a, b: b, left: left, right: right, session: session}
		pairs = append(pairs, p)
		warm, stop := context.WithTimeout(ctx, cfg.StepTimeout)
		f, e := left.send(warm, sendFrame(b.id, im.ReceiveTypeUser, "blackbox warmup"))
		if e == nil {
			e = stored(f)
		}
		if e == nil {
			_, e = right.waitMessage(warm, f.Result.Message.MessageID)
		}
		stop()
		if e != nil {
			return fail("load warmup live delivery failed")
		}
	}
	var mu sync.Mutex
	var attempts []loadAttempt
	var workers sync.WaitGroup
	tasks := make(chan struct{})
	begin := time.Now()
	measured, stopMeasure := context.WithTimeout(ctx, cfg.Duration)
	defer stopMeasure()
	for i := range pairs {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			p := &pairs[index]
			for range tasks {
				if measured.Err() != nil {
					return
				}
				sender, receiver, target := p.left, p.right, p.b.id
				if p.turn%2 == 1 {
					sender, receiver, target = p.right, p.left, p.a.id
				}
				p.turn++
				input := sendFrame(target, im.ReceiveTypeUser, "blackbox load")
				input.Content = "blackbox-load-" + input.ClientMessageID
				attempt := loadAttempt{input: input, receiver: receiver, start: time.Now(), pair: index}
				sendCtx, stop := context.WithTimeout(ctx, cfg.StepTimeout)
				attempt.result, attempt.err = sender.send(sendCtx, input)
				attempt.save = time.Since(attempt.start)
				stop()
				mu.Lock()
				attempts = append(attempts, attempt)
				mu.Unlock()
			}
		}(i)
	}
	ticker := time.NewTicker(time.Duration(float64(time.Second) / cfg.Rate))
	defer ticker.Stop()
dispatch:
	for {
		select {
		case <-measured.Done():
			break dispatch
		case <-ticker.C:
			select {
			case tasks <- struct{}{}:
			default:
				report.RateLimitedTicks++
			}
		}
	}
	close(tasks)
	report.MeasuredDurationMS = float64(time.Since(begin)) / float64(time.Millisecond)
	workers.Wait()
	report.Attempted = len(attempts)
	report.AchievedRate = float64(report.Attempted) / (report.MeasuredDurationMS / 1000)
	// One shared drain deadline prevents a missing peer push multiplying run time.
	drain, stopDrain := context.WithTimeout(ctx, cfg.ReceiveTimeout)
	for _, attempt := range attempts {
		if attempt.err == nil && stored(attempt.result) == nil {
			_, _ = attempt.receiver.waitMessage(drain, attempt.result.Result.Message.MessageID)
		}
	}
	stopDrain()
	// Freeze online observations before any repair; sync recovery is never live delivery.
	online := map[*socket]map[string]observation{}
	for _, p := range pairs {
		for _, s := range []*socket{p.left, p.right} {
			s.mu.Lock()
			snapshot := map[string]observation{}
			for id, o := range s.messages {
				snapshot[id] = o
			}
			online[s] = snapshot
			s.mu.Unlock()
		}
	}
	histories := make([]map[string]im.Message, len(pairs))
	for i, p := range pairs {
		repair, stop := context.WithTimeout(ctx, cfg.StepTimeout)
		messages, e := c.sync(repair, p.a, "direct:"+p.session, 500)
		stop()
		if e != nil {
			return fail("authoritative load repair failed")
		}
		histories[i] = map[string]im.Message{}
		for _, m := range messages {
			if strings.HasPrefix(m.Content, "blackbox-load-") {
				if _, exists := histories[i][m.Content]; exists {
					return fail("duplicate authoritative load input")
				}
				histories[i][m.Content] = m
			}
		}
	}
	var saveLatencies, receiveLatencies []time.Duration
	for _, attempt := range attempts {
		m, exists := histories[attempt.pair][attempt.input.Content]
		receiptKnown := attempt.err == nil && stored(attempt.result) == nil
		if attempt.err != nil {
			report.GeneratorErrors++
		}
		if receiptKnown {
			report.Stored++
			saveLatencies = append(saveLatencies, attempt.save)
			if !exists || m.MessageID != attempt.result.Result.Message.MessageID {
				report.Missing++
				continue
			}
		}
		if !receiptKnown {
			if exists {
				report.Stored++
			} else if attempt.err == nil && attempt.result.Error != nil && attempt.result.Error.Code != im.ErrorResultUnknown && attempt.result.Error.Code != im.ErrorDependencyUnavailable {
				report.Rejected++
				continue
			} else {
				report.Uncertain++
				continue
			}
		}
		o, received := online[attempt.receiver][m.MessageID]
		if received {
			if o.at.Before(attempt.start) {
				return fail("invalid monotonic receive timing")
			}
			report.Received++
			receiveLatencies = append(receiveLatencies, o.at.Sub(attempt.start))
			report.Duplicate += o.count - 1
		} else {
			report.Recovered++
		}
	}
	report.SaveLatency = summarize(saveLatencies)
	report.ReceiveLatency = summarize(receiveLatencies)
	report.StoredRate = float64(report.Stored) / (report.MeasuredDurationMS / 1000)
	if report.Stored > 0 {
		report.OnlineReceiveRatio = float64(report.Received) / float64(report.Stored)
	}
	if report.Attempted != report.Stored+report.Uncertain+report.Rejected || report.Received+report.Recovered+report.Missing != report.Stored {
		return fail("malformed load accounting")
	}
	if ctx.Err() != nil {
		return fail("load run canceled or overall timeout")
	}
	if report.Attempted == 0 || report.Stored == 0 {
		return fail("load produced no stored messages")
	}
	if report.Missing > 0 {
		return fail("stored messages missing after authoritative repair")
	}
	if report.Uncertain > 0 {
		return fail("send outcomes remain uncertain after repair")
	}
	if report.Rejected > 0 || report.GeneratorErrors > 0 {
		return fail("load sends rejected or generator failed")
	}
	if cfg.MaxSaveP95 > 0 && (report.SaveLatency.Samples == 0 || report.SaveLatency.P95MS > float64(cfg.MaxSaveP95)/float64(time.Millisecond)) {
		return fail("save latency threshold exceeded")
	}
	if cfg.MaxReceiveP95 > 0 && (report.ReceiveLatency.Samples == 0 || report.ReceiveLatency.P95MS > float64(cfg.MaxReceiveP95)/float64(time.Millisecond)) {
		return fail("online receive latency threshold exceeded")
	}
	if report.OnlineReceiveRatio < cfg.MinReceiveRatio {
		return fail("online receive ratio threshold not met")
	}
	report.Passed = true
	return report, nil
}
