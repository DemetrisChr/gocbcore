package gocbcore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errTelemetryNoEndpoints = errors.New("no telemetry endpoints available")
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

type telemetryEndpoints struct {
	epList    []routeEndpoint
	tlsConfig *dynTLSConfig
	auth      AuthProvider
}

func (e *telemetryEndpoints) selectEndpoint(excludeAddress string) (string, error) {
	if len(e.epList) == 0 {
		return "", errTelemetryNoEndpoints
	}
	if len(e.epList) == 1 {
		return e.epList[0].Address, nil
	}

	var candidates []string
	for _, ep := range e.epList {
		if ep.Address != excludeAddress {
			candidates = append(candidates, ep.Address)
		}
	}
	return candidates[rand.Intn(len(candidates))], nil // #nosec G404
}

func (e *telemetryEndpoints) createDialer(address string) (*websocket.Dialer, error) {
	var tlsConfig *tls.Config
	var err error

	if e.tlsConfig != nil {
		tlsConfig, err = e.tlsConfig.MakeForAddr(trimSchemePrefix(address))
		if err != nil {
			return nil, err
		}
	}

	return &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
		TLSClientConfig:  tlsConfig,
	}, nil
}

func (e *telemetryEndpoints) getAuthHeader(address string) (http.Header, error) {
	creds, err := e.auth.Credentials(AuthCredsRequest{Service: MgmtService, Endpoint: address})
	if err != nil {
		return nil, err
	}
	if len(creds) != 1 {
		return nil, errInvalidCredentials
	}

	return http.Header{
		"Authorization": []string{
			"Basic " + base64.StdEncoding.EncodeToString(
				[]byte(fmt.Sprintf("%s:%s", creds[0].Username, creds[0].Password))),
		},
	}, nil
}

type telemetryWebsocketClient struct {
	endpoints      atomic.Value
	fixedEndpoints bool

	backoff      time.Duration
	pingInterval time.Duration
	pingTimeout  time.Duration

	getMetricsFn func() string

	conn                *websocket.Conn
	connMutex           sync.Mutex
	active              bool
	lastEndpointAddress string

	shutdownSig   chan struct{}
	parentContext context.Context
}

func newTelemetryWebsocketClient(backoff, pingInterval, pingTimeout time.Duration, getMetricsFn func() string) *telemetryWebsocketClient {
	if backoff == time.Duration(0) {
		backoff = 5 * time.Second
	}
	if pingInterval == time.Duration(0) {
		pingInterval = 30 * time.Second
	}
	if pingTimeout == time.Duration(0) {
		pingTimeout = 5 * time.Second
	}

	w := &telemetryWebsocketClient{
		backoff:      backoff,
		pingInterval: pingInterval,
		pingTimeout:  pingTimeout,
		getMetricsFn: getMetricsFn,
		shutdownSig:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-w.shutdownSig
		cancel()
	}()

	w.parentContext = ctx

	return w
}

func (w *telemetryWebsocketClient) setEndpoints(endpoints telemetryEndpoints) {
	w.endpoints.Store(endpoints)
}

func (w *telemetryWebsocketClient) getEndpoints() telemetryEndpoints {
	return w.endpoints.Load().(telemetryEndpoints)
}

func (w *telemetryWebsocketClient) updateEndpoints(endpoints telemetryEndpoints) {
	if w.fixedEndpoints {
		// We shouldn't get here (telemetry components will not be listening to config updates in this case), but if
		// we do then ignore the update.
		return
	}

	w.setEndpoints(endpoints)
}

func (w *telemetryWebsocketClient) connect() {
	w.connMutex.Lock()

	if w.conn != nil {
		// There is already a connection, no need to connect
		w.connMutex.Unlock()
		return
	}

	conn, err := w.dialConn()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// The client has been closed, don't attempt to reconnect.
			w.connMutex.Unlock()
			return
		}
		if errors.Is(err, errTelemetryNoEndpoints) {
			// There are no telemetry endpoints, we shouldn't retry. If the endpoints are updated, connect will be called again.
			w.connMutex.Unlock()
			logWarnf("Failed to establish websocket connection for telemetry reporting. No app telemetry endpoints are available.")
			return
		}
		logWarnf("Failed to establish websocket connection for telemetry reporting, retrying in %s: %v", w.backoff, err)

		w.connMutex.Unlock() // Don't hold the lock while waiting
		w.reconnect()
		return
	}
	w.conn = conn

	w.connMutex.Unlock()

	go func() {
		err := w.readWritePump()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			logDebugf("Error from readWritePump: %s. Attempting to reconnect to app telemetry websocket in %s.", err, w.backoff)
			w.connMutex.Lock()
			w.conn = nil // Reset the connection pointer to allow attempts to reconnect.
			w.connMutex.Unlock()
			w.reconnect()
		}
	}()
}

func (w *telemetryWebsocketClient) reconnect() {
	select {
	case <-w.shutdownSig:
		logDebugf("Telemetry reporter shutting down, canceling connection attempt")
		return
	case <-time.After(w.backoff):
		w.connect()
	}
}

func (w *telemetryWebsocketClient) dialConn() (*websocket.Conn, error) {
	eps := w.getEndpoints()
	address, err := eps.selectEndpoint(w.lastEndpointAddress)
	if err != nil {
		return nil, wrapError(err, "failed to select app telemetry endpoint")
	}
	var header http.Header
	if !w.fixedEndpoints {
		header, err = eps.getAuthHeader(address)
		if err != nil {
			return nil, wrapError(err, "failed to create auth header")
		}
	}
	dialer, err := eps.createDialer(address)
	if err != nil {
		return nil, wrapError(err, "failed to create websocket dialer")
	}
	logDebugf("Connecting to app telemetry endpoint: dialing %s", address)
	conn, _, err := dialer.DialContext(w.parentContext, address, header) // nolint: bodyclose
	if err != nil {
		return nil, err
	}
	w.lastEndpointAddress = address

	return conn, nil
}

func (w *telemetryWebsocketClient) readWritePump() error {
	pingStopCh := make(chan struct{})
	defer close(pingStopCh)
	defer w.conn.Close()

	err := w.startPingTicker(pingStopCh)
	if err != nil {
		return err
	}

	for {
		_, message, err := w.conn.ReadMessage()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			logInfof("Error reading from telemetry reporter websocket: %v", err)
			return err
		}
		cmd := telemetryCommand(message[0])

		var resp telemetryResponse
		switch cmd {
		case telemetryCommandGetTelemetry:
			logSchedf("Received GET_TELEMETRY command from server telemetry collector")
			resp.status = telemetryStatusSuccess
			metrics := w.getMetricsFn()
			resp.data = []byte(metrics)
		default:
			logSchedf("Received unknown command from server telemetry collector")
			resp.status = telemetryStatusUnknownCommand
		}
		logSchedf("Sending telemetry response to server telemetry collector. Size=%d bytes", len(resp.data))
		err = w.conn.WriteMessage(websocket.BinaryMessage, resp.encode())
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			logInfof("Error writing to telemetry reporter websocket: %v", err)
			return err
		}
	}
}

func (w *telemetryWebsocketClient) startPingTicker(stopCh chan struct{}) error {
	err := w.conn.SetReadDeadline(time.Now().Add(w.pingInterval + w.pingTimeout))
	if err != nil {
		return wrapError(err, "Could not update read deadline")
	}

	lastPingTimestamp := time.Now().UnixMilli()

	w.conn.SetPongHandler(func(string) error {
		err := w.conn.SetReadDeadline(time.UnixMilli(atomic.LoadInt64(&lastPingTimestamp)).Add(w.pingInterval + w.pingTimeout))
		if err != nil {
			return wrapError(err, "Could not update read deadline in pong handler")
		}
		return nil
	})

	go func() {
		ticker := time.NewTicker(w.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				atomic.StoreInt64(&lastPingTimestamp, time.Now().UnixMilli())
				err := w.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(w.pingTimeout))
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					logInfof("Error writing PING message telemetry reporter websocket: %v", err)
					// No need to take any action on this error - reads will time out if we are unable to send pings, as
					// the read deadline will not be increased.
					return
				}
			}
		}
	}()

	return nil
}

func (w *telemetryWebsocketClient) Close() {
	close(w.shutdownSig)
}
