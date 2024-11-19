package gocbcore

import (
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type telemetryCommand uint8

const (
	telemetryCommandGetTelemetry = telemetryCommand(0x00)
)

type telemetryStatus uint8

const (
	telemetryStatusSuccess        = telemetryStatus(0x00)
	telemetryStatusUnknownCommand = telemetryStatus(0x01)
)

var (
	kvNonDurableThresholds = []time.Duration{
		1 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 1000 * time.Millisecond, 2500 * time.Millisecond,
	}

	kvDurableThresholds = []time.Duration{
		10 * time.Millisecond, 100 * time.Millisecond, 1 * time.Second, 2 * time.Second, 5 * time.Second,
	}

	nonKvThresholds = []time.Duration{
		100 * time.Millisecond, 1 * time.Second, 10 * time.Second, 30 * time.Second, 75 * time.Second,
	}
)

type latencyHistogramBin struct {
	lowerBound *time.Duration
	upperBound *time.Duration

	count *atomic.Uint64
}

func (b *latencyHistogramBin) IsWithin(duration time.Duration) bool {
	if b.lowerBound == nil && b.upperBound == nil {
		return true
	}
	if b.lowerBound == nil {
		return duration < *b.upperBound
	}
	if b.upperBound == nil {
		return *b.lowerBound <= duration
	}
	return *b.lowerBound <= duration && duration < *b.upperBound
}

type latencyHistogram struct {
	bins []latencyHistogramBin
}

func newLatencyHistogram(thresholds []time.Duration) *latencyHistogram {
	h := &latencyHistogram{}
	for idx := range thresholds {
		if idx == 0 {
			h.bins = append(h.bins, latencyHistogramBin{
				lowerBound: nil,
				upperBound: &thresholds[idx],
			})
		}
		if idx == len(thresholds)-1 {
			h.bins = append(h.bins, latencyHistogramBin{
				lowerBound: &thresholds[idx],
				upperBound: nil,
			})
		}
		h.bins = append(h.bins, latencyHistogramBin{
			lowerBound: &thresholds[idx],
			upperBound: &thresholds[idx+1],
		})

	}
	return h
}

func (h *latencyHistogram) AddValue(value time.Duration) {
	for _, bin := range h.bins {
		if bin.IsWithin(value) {
			bin.count.Add(1)
		}
	}
}

type telemetryCounters struct {
	all                *telemetryCounterMap
	unambiguousTimeout *telemetryCounterMap
	ambiguousTimeout   *telemetryCounterMap
	canceled           *telemetryCounterMap
}

func newTelemetryCounters() *telemetryCounters {
	return &telemetryCounters{
		all:                newTelemetryCounterMap(),
		unambiguousTimeout: newTelemetryCounterMap(),
		ambiguousTimeout:   newTelemetryCounterMap(),
		canceled:           newTelemetryCounterMap(),
	}
}

func (c *telemetryCounters) recordOp(attributes TelemetryOperationAttributes) {
	key := telemetryCounterKey{
		service: attributes.service,
		node:    attributes.node,
		bucket:  attributes.bucket,
	}

	if attributes.outcome != nil {
		if errors.Is(attributes.outcome, ErrUnambiguousTimeout) {
			c.unambiguousTimeout.Increment(key)
		}
		if errors.Is(attributes.outcome, ErrAmbiguousTimeout) {
			c.ambiguousTimeout.Increment(key)
		}
		if errors.Is(attributes.outcome, ErrRequestCanceled) {
			c.canceled.Increment(key)
		}
	}
	c.all.Increment(key)
}

func (c *telemetryCounters) serialize(timestamp int64, agent string) []string {
	var entries []string

	entries = append(entries, c.all.serialize(timestamp, "total", agent)...)
	entries = append(entries, c.unambiguousTimeout.serialize(timestamp, "utimedout", agent)...)
	entries = append(entries, c.ambiguousTimeout.serialize(timestamp, "atimedout", agent)...)
	entries = append(entries, c.canceled.serialize(timestamp, "canceled", agent)...)

	return entries
}

type telemetryLatencyHistograms struct {
	kvRetrieval          *telemetryLatencyHistogramMap
	kvNonDurableMutation *telemetryLatencyHistogramMap
	kvDurableMutation    *telemetryLatencyHistogramMap
	query                *telemetryLatencyHistogramMap
	search               *telemetryLatencyHistogramMap
	analytics            *telemetryLatencyHistogramMap
}

func newTelemetryLatencyHistograms() *telemetryLatencyHistograms {

	return &telemetryLatencyHistograms{
		kvRetrieval:          newTelemetryLatencyHistogramMap(kvNonDurableThresholds),
		kvNonDurableMutation: newTelemetryLatencyHistogramMap(kvNonDurableThresholds),
		kvDurableMutation:    newTelemetryLatencyHistogramMap(kvDurableThresholds),
		query:                newTelemetryLatencyHistogramMap(nonKvThresholds),
		search:               newTelemetryLatencyHistogramMap(nonKvThresholds),
		analytics:            newTelemetryLatencyHistogramMap(nonKvThresholds),
	}
}

func (h *telemetryLatencyHistograms) serialize(timestamp int64, agent string) []string {
	var entries []string

	return entries
}

func (h *telemetryLatencyHistograms) recordOp(duration time.Duration, attributes TelemetryOperationAttributes) {
	key := telemetryHistogramKey{
		node:   attributes.node,
		bucket: attributes.bucket,
	}

	switch attributes.service {
	case MemdService:
		if attributes.isNonDurableMutation {
			h.kvNonDurableMutation.AddValue(key, duration)
		} else if attributes.isDurableMutation {
			h.kvDurableMutation.AddValue(key, duration)
		} else {
			h.kvRetrieval.AddValue(key, duration)
		}
	case N1qlService:
		h.query.AddValue(key, duration)
	case FtsService:
		h.search.AddValue(key, duration)
	case CbasService:
		h.analytics.AddValue(key, duration)
	}
}

type telemetryCounterKey struct {
	service ServiceType
	node    string

	// KV-only
	bucket string
}

func (k *telemetryCounterKey) serviceAsString() string {
	switch k.service {
	case MemdService:
		return "kv"
	case N1qlService:
		return "query"
	case FtsService:
		return "search"
	case CbasService:
		return "analytics"
	}
	return ""
}

type telemetryHistogramKey struct {
	node string

	// KV-only
	bucket string
}

type telemetryLatencyHistogramMap struct {
	thresholds      []time.Duration
	histograms      map[telemetryHistogramKey]*latencyHistogram
	histogramsMutex sync.Mutex
}

func newTelemetryLatencyHistogramMap(thresholds []time.Duration) *telemetryLatencyHistogramMap {
	return &telemetryLatencyHistogramMap{
		thresholds: thresholds,
		histograms: make(map[telemetryHistogramKey]*latencyHistogram),
	}
}

func (m *telemetryLatencyHistogramMap) AddValue(key telemetryHistogramKey, value time.Duration) {
	var hist *latencyHistogram

	m.histogramsMutex.Lock()
	hist, ok := m.histograms[key]
	if !ok {
		hist = newLatencyHistogram(m.thresholds)
		m.histograms[key] = hist
	}
	m.histogramsMutex.Unlock()

	hist.AddValue(value)
}

type telemetryCounterMap struct {
	counters      map[telemetryCounterKey]*atomic.Uint64
	countersMutex sync.Mutex
}

func (m *telemetryCounterMap) Increment(key telemetryCounterKey) {
	var counter *atomic.Uint64

	m.countersMutex.Lock()
	counter, ok := m.counters[key]
	if !ok {
		counter = &atomic.Uint64{}
		m.counters[key] = counter
	}
	m.countersMutex.Unlock()

	counter.Add(1)
}

func (m *telemetryCounterMap) serialize(timestamp int64, counterName, agent string) []string {
	var entries []string

	for k, v := range m.counters {
		tags := fmt.Sprintf("agent=\"%s\", node=\"%s\"", agent, k.node)
		if k.service == MemdService {
			tags += fmt.Sprintf(", bucket=\"%s\"", k.bucket)
		}
		entries = append(entries, fmt.Sprintf("sdk_%s_r_%s{%s} %d %d", k.serviceAsString(), counterName, tags, v.Load(), timestamp))
	}

	return entries
}

func newTelemetryCounterMap() *telemetryCounterMap {
	return &telemetryCounterMap{
		counters: make(map[telemetryCounterKey]*atomic.Uint64),
	}
}

type telemetryMetrics struct {
	counters   *telemetryCounters
	histograms *telemetryLatencyHistograms

	agent              string
	reportingTimestamp int64
}

func newTelemetryMetrics(agent string) *telemetryMetrics {
	tm := &telemetryMetrics{
		counters:   newTelemetryCounters(),
		histograms: newTelemetryLatencyHistograms(),
		agent:      agent,
	}

	return tm
}

func (tm *telemetryMetrics) serialize() string {
	tm.reportingTimestamp = time.Now().Unix()

	var entries []string
	entries = append(entries, tm.counters.serialize(tm.reportingTimestamp, tm.agent)...)
	entries = append(entries, tm.histograms.serialize(tm.reportingTimestamp, tm.agent)...)

	return strings.Join(entries, "\n")
}

func (tm *telemetryMetrics) recordOp(duration time.Duration, attributes TelemetryOperationAttributes) {
	if !attributes.IsValid() {
		return
	}

	tm.counters.recordOp(attributes)
	tm.histograms.recordOp(duration, attributes)
}

type TelemetryReporter interface {
	Close() error
}

type TelemetryOperationAttributes struct {
	outcome error
	node    string

	service ServiceType

	// For KV operations
	bucket               string
	isNonDurableMutation bool
	isDurableMutation    bool
}

func (a *TelemetryOperationAttributes) IsValid() bool {
	if a.service != MemdService && a.service != N1qlService && a.service != FtsService && a.service != CbasService {
		// We only record metrics for kv, query, search, analytics
		return false
	}
	return true
}

type TelemetryStore interface {
	ExportMetrics() string
}

type websocketTelemetryReporter struct {
	endpoints []string
	backoff   time.Duration
	store     TelemetryStore

	conn             *websocket.Conn
	selectedEndpoint string

	cfgMgr *configManager
}

type telemetryResponse struct {
	status telemetryStatus
	data   []byte
}

func (r *telemetryResponse) encode() []byte {
	encoded := r.data
	encoded = append(encoded, byte(0))
	copy(encoded[1:], encoded)
	encoded[0] = byte(r.status)
	return encoded
}

func (r *websocketTelemetryReporter) selectEndpoint() error {
	if len(r.endpoints) == 0 {
		return errors.New("no available app telemetry endpoints")
	}
	if len(r.endpoints) == 1 {
		r.selectedEndpoint = r.endpoints[0]
		return nil
	}
	var allowlist []string
	for _, ep := range r.endpoints {
		// Exclude the last selected endpoint
		if ep != r.selectedEndpoint {
			allowlist = append(allowlist, ep)
		}
	}
	r.selectedEndpoint = allowlist[rand.Intn(len(allowlist))]
	return nil
}

func (r *websocketTelemetryReporter) connect() error {
	err := r.selectEndpoint()
	if err != nil {
		return err
	}

	r.conn, _, err = websocket.DefaultDialer.Dial(r.selectedEndpoint, nil)
	if err != nil {
		return wrapError(err, "failed to establish websocket connection")
	}

	go r.doLoop()

	return nil
}

func (r *websocketTelemetryReporter) doLoop() {
	for {
		_, message, err := r.conn.ReadMessage()
		if err != nil {
			// TODO: Handle error
			break
		}
		cmd := telemetryCommand(message[0])
		var resp telemetryResponse
		switch cmd {
		case telemetryCommandGetTelemetry:
			resp.status = telemetryStatusSuccess
			resp.data = []byte(r.store.ExportMetrics())
		default:
			resp.status = telemetryStatusUnknownCommand
		}
		err = r.conn.WriteMessage(websocket.BinaryMessage, resp.encode())
		if err != nil {
			// TODO: Handle error
			break
		}
	}
}

func (r *websocketTelemetryReporter) Close() error {
	err := r.conn.Close()
	if err != nil {
		return wrapError(err, "failed to close telemetry reporter websocket connection")
	}
	return nil
}

func (r *websocketTelemetryReporter) OnNewRouteConfig(cfg *routeConfig) {
}

type telemetryComponent struct {
	reporter TelemetryReporter

	metrics      *telemetryMetrics
	metricsMutex sync.RWMutex

	userAgent string
	backoff   time.Duration
}

type telemetryComponentProps struct {
	userAgent string
	backoff   time.Duration
}

func newTelemetryComponent(props telemetryComponentProps) *telemetryComponent {
	tc := &telemetryComponent{
		metrics:   newTelemetryMetrics(props.userAgent),
		userAgent: props.userAgent,
		backoff:   props.backoff,
	}

	return tc
}

func (tc *telemetryComponent) setupWebsocketReporterWithFixedEndpoint(endpoint string) error {
	reporter := &websocketTelemetryReporter{
		endpoints: []string{endpoint},
		backoff:   tc.backoff,
		store:     tc,
	}

	err := reporter.connect()
	if err != nil {
		return wrapError(err, "failed to establish connection for app telemetry reporting")
	}

	tc.reporter = reporter
	return nil
}

func (tc *telemetryComponent) setupWebsocketReporter(cfgMgr configManager) {
	reporter := &websocketTelemetryReporter{
		backoff: tc.backoff,
		store:   tc,
	}
	cfgMgr.AddConfigWatcher(reporter)
	tc.reporter = reporter
}

func (tc *telemetryComponent) getAndResetMetrics() *telemetryMetrics {
	m := newTelemetryMetrics(tc.userAgent)

	tc.metricsMutex.Lock()
	m, tc.metrics = tc.metrics, m
	tc.metricsMutex.Unlock()

	return m
}

func (tc *telemetryComponent) ExportMetrics() string {
	metrics := tc.getAndResetMetrics()
	return metrics.serialize()
}

func (tc *telemetryComponent) RecordOp(duration time.Duration, attributes TelemetryOperationAttributes) {
	tc.metricsMutex.RLock()
	tc.metrics.recordOp(duration, attributes)
	tc.metricsMutex.RUnlock()
}

func (tc *telemetryComponent) Close() error {
	return tc.reporter.Close()
}
