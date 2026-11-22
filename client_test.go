/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package getty

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

import (
	"github.com/gorilla/websocket"

	"github.com/stretchr/testify/assert"
)

type PackageHandler struct{}

func (h *PackageHandler) Read(ss Session, data []byte) (any, int, error) {
	return nil, 0, nil
}

func (h *PackageHandler) Write(ss Session, pkg any) ([]byte, error) {
	return nil, nil
}

type MessageHandler struct {
	lock  sync.Mutex
	array []Session
}

type sessionCloseMessageHandler struct {
	MessageHandler
	closed chan struct{}
}

func (h *sessionCloseMessageHandler) OnClose(session Session) {
	select {
	case h.closed <- struct{}{}:
	default:
	}
}

func (h *MessageHandler) SessionNumber() int {
	h.lock.Lock()
	connNum := len(h.array)
	h.lock.Unlock()

	return connNum
}

func (h *MessageHandler) OnOpen(session Session) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.array = append(h.array, session)

	return nil
}
func (h *MessageHandler) OnError(session Session, err error) {}
func (h *MessageHandler) OnClose(session Session)            {}
func (h *MessageHandler) OnMessage(session Session, pkg any) {}
func (h *MessageHandler) OnCron(session Session)             {}

type poolClosingMessageHandler struct {
	MessageHandler
	client *client
	opened chan Session
}

func (h *poolClosingMessageHandler) OnOpen(session Session) error {
	h.client.Lock()
	h.client.ssMap = nil
	h.client.Unlock()
	h.opened <- session

	return h.MessageHandler.OnOpen(session)
}

type Package struct{}

func (p Package) String() string {
	return ""
}
func (p Package) Marshal() (*bytes.Buffer, error)           { return nil, nil }
func (p *Package) Unmarshal(buf *bytes.Buffer) (int, error) { return 0, nil }

func newSessionCallback(session Session, handler *MessageHandler) error {
	var pkgHandler PackageHandler
	session.SetName("hello-client-session")
	session.SetMaxMsgLen(128 * 1024) // max message package length 128k
	session.SetPkgHandler(&pkgHandler)
	session.SetEventListener(handler)
	session.SetReadTimeout(3e9)
	session.SetWriteTimeout(3e9)
	session.SetCronPeriod((int)(30e9 / 1e6))
	session.SetWaitTime(3e9)

	return nil
}

var errTestTLSConfig = errors.New("test TLS config failure")

type countingTLSConfigBuilder struct {
	calls   atomic.Int32
	entered chan struct{}
	release <-chan struct{}
	exited  chan struct{}
}

func (b *countingTLSConfigBuilder) BuildTlsConfig() (*tls.Config, error) {
	b.calls.Add(1)
	if b.entered != nil {
		select {
		case b.entered <- struct{}{}:
		default:
		}
	}
	if b.release != nil {
		<-b.release
	}
	if b.exited != nil {
		select {
		case b.exited <- struct{}{}:
		default:
		}
	}
	return nil, errTestTLSConfig
}

func newFailingReconnectClient(builder TlsConfigBuilder, interval time.Duration, attempts int) *client {
	return newClient(
		TCP_CLIENT,
		WithServerAddress("127.0.0.1:1"),
		WithConnectionNumber(1),
		WithReconnectInterval(int(interval)),
		WithReconnectAttempts(attempts),
		WithClientSslEnabled(true),
		WithClientTlsConfigBuilder(builder),
	)
}

func newBlockingReconnectClient(t *testing.T) (*client, *MessageHandler, <-chan struct{}, func(), *atomic.Int32) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	assert.NotNil(t, listener)

	acceptedConns := make(chan net.Conn, 8)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case acceptedConns <- conn:
			default:
				_ = conn.Close()
			}
		}
	}()

	clt := newClient(
		TCP_CLIENT,
		WithServerAddress(listener.Addr().String()),
		WithConnectionNumber(1),
		WithReconnectInterval(int(time.Millisecond)),
		WithReconnectAttempts(2),
	)
	var (
		callbacks       atomic.Int32
		callbackEntered = make(chan struct{}, 1)
		releaseCallback = make(chan struct{})
		releaseOnce     sync.Once
		msgHandler      sessionCloseMessageHandler
	)
	msgHandler.closed = make(chan struct{}, 1)
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCallback)
		})
	}
	clt.newSession = func(session Session) error {
		callbacks.Add(1)
		select {
		case callbackEntered <- struct{}{}:
		default:
		}
		<-releaseCallback
		if err := newSessionCallback(session, &msgHandler.MessageHandler); err != nil {
			return err
		}
		session.SetEventListener(&msgHandler)
		return nil
	}
	t.Cleanup(func() {
		release()
		_ = listener.Close()
		<-acceptDone
		closedAcceptedConn := false
		for {
			select {
			case conn := <-acceptedConns:
				closedAcceptedConn = true
				_ = conn.Close()
			default:
				if closedAcceptedConn {
					select {
					case <-msgHandler.closed:
					case <-time.After(time.Second):
					}
				}
				clt.Close()
				return
			}
		}
	})

	return clt, &msgHandler.MessageHandler, callbackEntered, release, &callbacks
}

func TestTCPClientReconnectRunsSingleFlight(t *testing.T) {
	clt, msgHandler, callbackEntered, release, callbacks := newBlockingReconnectClient(t)

	firstDone := clt.runReconnect()
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("first reconnect did not enter NewSessionCallback")
	}

	secondDone := clt.runReconnect()
	if secondDone != firstDone {
		t.Fatal("concurrent reconnect trigger did not share the active worker")
	}
	assert.Equal(t, int32(1), callbacks.Load())
	select {
	case <-callbackEntered:
		t.Fatal("concurrent reconnect trigger entered a second NewSessionCallback")
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("single-flight reconnect did not finish")
	}

	assert.Equal(t, int32(1), callbacks.Load())
	assert.Equal(t, 1, msgHandler.SessionNumber())
	assert.Equal(t, 1, clt.sessionNum())
}

func TestSessionTriggeredReconnectIsCoalesced(t *testing.T) {
	clt, msgHandler, callbackEntered, release, callbacks := newBlockingReconnectClient(t)

	firstDone := clt.runReconnect()
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("first reconnect did not enter NewSessionCallback")
	}

	localConn, peerConn := net.Pipe()
	defer func() {
		_ = localConn.Close()
		_ = peerConn.Close()
	}()
	ss := newTCPSession(localConn, clt).(*session)
	ss.SetAttribute(sessionClientKey, clt)
	ss.SetAttribute(ignoreReconnectKey, false)
	ss.stop()

	clt.Lock()
	activeDone := clt.reconnectDone
	pending := clt.reconnectPending
	clt.Unlock()
	if activeDone != firstDone {
		t.Fatal("session-triggered reconnect did not share the active worker")
	}
	assert.True(t, pending)
	assert.Equal(t, int32(1), callbacks.Load())
	select {
	case <-callbackEntered:
		t.Fatal("session-triggered reconnect entered a second NewSessionCallback")
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("coalesced reconnect did not finish")
	}

	assert.Equal(t, int32(1), callbacks.Load())
	assert.Equal(t, 1, msgHandler.SessionNumber())
	assert.Equal(t, 1, clt.sessionNum())
}

func TestReconnectTriggerDuringBackoffIsCoalesced(t *testing.T) {
	builder := &countingTLSConfigBuilder{
		entered: make(chan struct{}, 2),
		exited:  make(chan struct{}, 2),
	}
	clt := newFailingReconnectClient(builder, 200*time.Millisecond, 2)
	firstDone := clt.runReconnect()

	select {
	case <-builder.entered:
	case <-time.After(time.Second):
		t.Fatal("first reconnect attempt did not start")
	}
	select {
	case <-builder.exited:
	case <-time.After(time.Second):
		t.Fatal("first reconnect attempt did not finish")
	}

	secondDone := clt.runReconnect()
	if secondDone != firstDone {
		t.Fatal("backoff trigger did not share the active reconnect worker")
	}

	select {
	case <-builder.entered:
		t.Fatal("backoff trigger started a second reconnect worker too early")
	case <-time.After(50 * time.Millisecond):
	}

	assert.Equal(t, int32(1), builder.calls.Load())

	clt.Close()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("reconnect worker did not finish after Close")
	}
}

func TestReconnectBackoffIsCancelledByClose(t *testing.T) {
	builder := &countingTLSConfigBuilder{entered: make(chan struct{}, 4)}
	clt := newFailingReconnectClient(builder, 2*time.Second, 3)

	eventLoopDone := make(chan struct{})
	go func() {
		clt.RunEventLoop(func(Session) error { return nil })
		close(eventLoopDone)
	}()
	select {
	case <-builder.entered:
	case <-time.After(time.Second):
		t.Fatal("first reconnect attempt did not start")
	}
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		clt.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(200 * time.Millisecond):
		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not return after the current backoff elapsed")
		}
		t.Fatal("Close did not cancel the reconnect backoff")
	}
	select {
	case <-eventLoopDone:
	case <-time.After(time.Second):
		t.Fatal("RunEventLoop did not return after Close")
	}
}

func TestRunReconnectAfterCloseIsNoop(t *testing.T) {
	builder := &countingTLSConfigBuilder{entered: make(chan struct{}, 1)}
	clt := newFailingReconnectClient(builder, time.Millisecond, 1)
	clt.Close()

	done := clt.runReconnect()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("runReconnect did not return immediately after Close")
	}
	assert.Equal(t, int32(0), builder.calls.Load())
}

func TestSessionTriggeredReconnectDoesNotBlockClose(t *testing.T) {
	releaseReconnect := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseReconnect)
		})
	}
	defer release()

	builder := &countingTLSConfigBuilder{
		entered: make(chan struct{}, 1),
		release: releaseReconnect,
		exited:  make(chan struct{}, 1),
	}
	clt := newFailingReconnectClient(builder, 2*time.Second, 3)

	localConn, peerConn := net.Pipe()
	defer func() {
		_ = localConn.Close()
		_ = peerConn.Close()
	}()
	ss := newTCPSession(localConn, clt).(*session)
	ss.SetAttribute(sessionClientKey, clt)
	ss.SetAttribute(ignoreReconnectKey, false)

	ss.stop()
	select {
	case <-builder.entered:
	case <-time.After(time.Second):
		t.Fatal("session-triggered reconnect did not start")
	}

	closeDone := make(chan struct{})
	go func() {
		clt.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Close blocked on the session-triggered reconnect worker")
	}
	release()
	select {
	case <-builder.exited:
	case <-time.After(time.Second):
		t.Fatal("session-triggered reconnect did not leave the blocked point after release")
	}
}

func TestNewSessionCallbackCloseDoesNotDeadlock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	assert.NotNil(t, listener)
	accepted := make(chan struct{}, 1)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- struct{}{}:
			default:
			}
			_ = conn.Close()
		}
	}()
	defer func() {
		_ = listener.Close()
		<-acceptDone
	}()

	clt := newClient(
		TCP_CLIENT,
		WithServerAddress(listener.Addr().String()),
		WithConnectionNumber(1),
		WithReconnectInterval(int(time.Millisecond)),
		WithReconnectAttempts(1),
	)
	callbackEntered := make(chan struct{})
	runDone := make(chan struct{})
	go func() {
		clt.RunEventLoop(func(Session) error {
			close(callbackEntered)
			clt.Close()
			return errors.New("reject session and stop client")
		})
		close(runDone)
	}()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept the client connection")
	}
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("NewSessionCallback did not run")
	}
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("RunEventLoop did not return after Close from NewSessionCallback")
	}
}

func TestTCPClient(t *testing.T) {
	listenLocalServer := func() (net.Listener, error) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}

		go func() {
			_ = http.Serve(listener, nil)
		}()
		return listener, nil
	}

	listener, err := listenLocalServer()
	assert.Nil(t, err)
	assert.NotNil(t, listener)

	addr := listener.Addr().(*net.TCPAddr)
	t.Logf("server addr: %v", addr)
	clt := NewTCPClient(
		WithServerAddress(addr.String()),
		WithReconnectInterval(5e8),
		WithConnectionNumber(1),
		WithReconnectAttempts(10),
	)
	assert.NotNil(t, clt)
	assert.True(t, clt.ID() > 0)
	// assert.Equal(t, clt.endPointType, TCP_CLIENT)

	var msgHandler MessageHandler
	cb := func(session Session) error {
		return newSessionCallback(session, &msgHandler)
	}

	clt.RunEventLoop(cb)
	time.Sleep(1e9)

	assert.Equal(t, 1, msgHandler.SessionNumber())
	ss := msgHandler.array[0]
	ss.SetCompressType(CompressNone)
	conn := ss.(*session).Connection.(*gettyTCPConn)
	assert.True(t, conn.getCompressType() == CompressNone)
	beforeWriteBytes := conn.writeBytes
	beforeWritePkgNum := conn.writePkgNum
	l, err := conn.Send([]byte("hello"))
	assert.Nil(t, err)
	assert.True(t, l == 5)
	beforeWritePkgNum.Add(1)
	beforeWriteBytes.Add(5)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	l, err = ss.WriteBytes([]byte("hello"))
	assert.Nil(t, err)
	assert.True(t, l == 5)
	beforeWriteBytes.Add(5)
	beforeWritePkgNum.Add(1)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)
	var pkgs [][]byte
	pkgs = append(pkgs, []byte("hello"), []byte("hello"))
	l, err = conn.Send(pkgs)
	assert.Nil(t, err)
	assert.True(t, l == 10)
	beforeWritePkgNum.Add(2)
	beforeWriteBytes.Add(10)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	ss.SetCompressType(CompressSnappy)
	// net.Buffer.WriteTo would consume the memory of pkgs, we should allocate another one for testing
	var anotherPkgs [][]byte
	anotherPkgs = append(anotherPkgs, []byte("hello"), []byte("hello"))
	l, err = ss.WriteBytesArray(anotherPkgs...)
	assert.Nil(t, err)
	assert.True(t, l == 10)
	beforeWritePkgNum.Add(2)
	beforeWriteBytes.Add(10)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	assert.True(t, conn.getCompressType() == CompressSnappy)

	batchSize := 128 * 1023
	source := make([]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		source[i] = 't'
	}
	l, err = ss.WriteBytes(source)
	assert.Nil(t, err)
	assert.True(t, l == batchSize)
	beforeWriteBytes.Add(uint32(batchSize))
	beforeWritePkgNum.Add(uint32(batchSize/16/1024) + 1)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)

	batchSize = 32 * 1024
	source = make([]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		source[i] = 't'
	}
	l, err = ss.WriteBytes(source)
	assert.Nil(t, err)
	assert.True(t, l == batchSize)
	beforeWriteBytes.Add(uint32(batchSize))
	beforeWritePkgNum.Add(2)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)

	clt.Close()
	assert.True(t, clt.IsClosed())
}

func TestTCPClientReconnectStopsAfterMaxAttempts(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	assert.NotNil(t, listener)
	addr := listener.Addr().String()
	assert.Nil(t, listener.Close())

	clt := NewTCPClient(
		WithServerAddress(addr),
		WithReconnectInterval(int(time.Millisecond)),
		WithConnectionNumber(1),
		WithReconnectAttempts(2),
	)
	assert.NotNil(t, clt)

	done := make(chan struct{})
	go func() {
		var msgHandler MessageHandler
		cb := func(session Session) error {
			return newSessionCallback(session, &msgHandler)
		}
		clt.RunEventLoop(cb)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("client reconnect loop did not stop after max reconnect attempts")
	}

	clt.Close()
	assert.True(t, clt.IsClosed())
}

func TestTCPClientReconnectBacksOffWhenNewSessionFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	assert.NotNil(t, listener)

	acceptDone := make(chan struct{})
	accepted := make(chan struct{}, 2)
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- struct{}{}:
			default:
			}
			_ = conn.Close()
		}
	}()
	defer func() {
		_ = listener.Close()
		<-acceptDone
	}()

	clt := NewTCPClient(
		WithServerAddress(listener.Addr().String()),
		WithReconnectInterval(int(time.Millisecond)),
		WithConnectionNumber(1),
		WithReconnectAttempts(2),
	)
	assert.NotNil(t, clt)

	done := make(chan struct{})
	attempts := 0
	go func() {
		cb := func(session Session) error {
			attempts++
			return errors.New("test new session failure")
		}
		clt.RunEventLoop(cb)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client reconnect loop did not stop when newSession kept failing")
	}

	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("server did not accept client connection")
		}
	}
	assert.Equal(t, 2, attempts)
	clt.Close()
	assert.True(t, clt.IsClosed())
}

func TestTCPClientConnectClosesSessionWhenPoolClosed(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	assert.NotNil(t, listener)
	defer func() {
		_ = listener.Close()
	}()

	acceptedConn := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		acceptedConn <- conn
	}()

	clt := NewTCPClient(
		WithServerAddress(listener.Addr().String()),
		WithConnectionNumber(1),
	).(*client)
	defer clt.Close()

	handler := &poolClosingMessageHandler{
		client: clt,
		opened: make(chan Session, 1),
	}
	clt.newSession = func(session Session) error {
		var pkgHandler PackageHandler
		session.SetName("pool-closing-client-session")
		session.SetPkgHandler(&pkgHandler)
		session.SetEventListener(handler)
		session.SetReadTimeout(time.Millisecond)
		session.SetWriteTimeout(time.Millisecond)
		session.SetWaitTime(time.Millisecond)

		return nil
	}

	assert.False(t, clt.connect())

	select {
	case session := <-handler.opened:
		assert.True(t, session.IsClosed())
	case <-time.After(time.Second):
		t.Fatal("session was not opened")
	}

	select {
	case conn := <-acceptedConn:
		_ = conn.Close()
	case err := <-acceptErr:
		t.Fatalf("server accept failed: %+v", err)
	case <-time.After(time.Second):
		t.Fatal("server did not accept client connection")
	}
}

func TestTCPClientReconnectAttemptsIgnoreSuccessfulPoolFills(t *testing.T) {
	const connPoolSize = 3
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	assert.NotNil(t, listener)
	addr := listener.Addr().String()
	assert.Nil(t, listener.Close())

	var (
		serverLock sync.Mutex
		listeners  []net.Listener
		conns      []net.Conn
		acceptDone <-chan struct{}
		serverErr  = make(chan error, connPoolSize)
	)
	defer func() {
		serverLock.Lock()
		defer serverLock.Unlock()
		for _, ln := range listeners {
			_ = ln.Close()
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()

	startServer := func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			serverErr <- err
			return
		}

		done := make(chan struct{})
		serverLock.Lock()
		listeners = append(listeners, ln)
		acceptDone = done
		serverLock.Unlock()

		go func() {
			conn, err := ln.Accept()
			_ = ln.Close()
			if err == nil {
				serverLock.Lock()
				conns = append(conns, conn)
				serverLock.Unlock()
			}
			close(done)
		}()
	}

	waitAccepted := func() {
		serverLock.Lock()
		done := acceptDone
		acceptDone = nil
		serverLock.Unlock()
		if done == nil {
			t.Fatal("client connected before the test server was ready")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("server did not accept client connection")
		}
	}

	time.AfterFunc(10*time.Millisecond, startServer)

	clt := NewTCPClient(
		WithServerAddress(addr),
		WithReconnectInterval(int(50*time.Millisecond)),
		WithConnectionNumber(connPoolSize),
		WithReconnectAttempts(2),
	)
	assert.NotNil(t, clt)
	defer clt.Close()

	var (
		msgHandler MessageHandler
		successes  int
	)
	cb := func(session Session) error {
		waitAccepted()
		successes++
		if successes < connPoolSize {
			time.AfterFunc(10*time.Millisecond, startServer)
		}
		return newSessionCallback(session, &msgHandler)
	}

	done := make(chan struct{})
	go func() {
		clt.RunEventLoop(cb)
		close(done)
	}()

	select {
	case <-done:
	case err := <-serverErr:
		t.Fatalf("failed to start test server: %+v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("client reconnect loop did not complete")
	}

	assert.Equal(t, connPoolSize, msgHandler.SessionNumber())
}

func TestUDPClient(t *testing.T) {
	var (
		err      error
		conn     *net.UDPConn
		sendLen  int
		totalLen int
	)
	func() {
		ip := net.ParseIP("127.0.0.1")
		srcAddr := &net.UDPAddr{IP: ip, Port: 0}
		conn, err = net.ListenUDP("udp", srcAddr)
		assert.Nil(t, err)
		assert.NotNil(t, conn)
	}()
	defer func() { _ = conn.Close() }()

	addr := conn.LocalAddr()
	t.Logf("server addr: %v", addr)
	clt := NewUDPClient(
		WithServerAddress(addr.String()),
		WithReconnectInterval(5e8),
		WithConnectionNumber(1),
	)
	assert.NotNil(t, clt)
	assert.True(t, clt.ID() > 0)
	// assert.Equal(t, clt.endPointType, UDP_CLIENT)

	var msgHandler MessageHandler
	cb := func(session Session) error {
		return newSessionCallback(session, &msgHandler)
	}

	clt.RunEventLoop(cb)
	time.Sleep(1e9)

	assert.Equal(t, 1, msgHandler.SessionNumber())
	ss := msgHandler.array[0]
	totalLen, sendLen, err = ss.WritePkg(nil, 0)
	assert.NotNil(t, err)
	assert.True(t, sendLen == 0)
	assert.True(t, totalLen == 0)
	totalLen, sendLen, err = ss.WritePkg([]byte("hello"), 0)
	assert.NotNil(t, err)
	assert.True(t, sendLen == 0)
	assert.True(t, totalLen == 0)
	l, err := ss.WriteBytes([]byte("hello"))
	assert.Zero(t, l)
	assert.NotNil(t, err)
	l, err = ss.WriteBytesArray([]byte("hello"))
	assert.Zero(t, l)
	assert.NotNil(t, err)
	l, err = ss.WriteBytesArray([]byte("hello"), []byte("world"))
	assert.Zero(t, l)
	assert.NotNil(t, err)
	ss.SetCompressType(CompressNone)
	host, port, _ := net.SplitHostPort(addr.String())
	if len(host) < 8 {
		host = "127.0.0.1"
	}
	remotePort, _ := strconv.Atoi(port)
	serverAddr := net.UDPAddr{IP: net.ParseIP(host), Port: remotePort}
	udpCtx := UDPContext{
		Pkg:      "hello",
		PeerAddr: &serverAddr,
	}
	t.Logf("udp context:%s", udpCtx)
	udpConn := ss.(*session).Connection.(*gettyUDPConn)
	_, err = udpConn.Send(udpCtx)
	assert.NotNil(t, err)
	udpCtx.Pkg = []byte("hello")
	beforeWriteBytes := udpConn.writeBytes
	_, err = udpConn.Send(udpCtx)
	beforeWriteBytes.Add(5)
	assert.Equal(t, beforeWriteBytes, udpConn.writeBytes)
	assert.Nil(t, err)

	beforeWritePkgNum := udpConn.writePkgNum
	totalLen, sendLen, err = ss.WritePkg(udpCtx, 0)
	beforeWritePkgNum.Add(1)
	assert.Equal(t, beforeWritePkgNum, udpConn.writePkgNum)
	assert.Nil(t, err)
	assert.True(t, sendLen == 0)
	assert.True(t, totalLen == 0)

	clt.Close()
	assert.True(t, clt.IsClosed())
	msgHandler.array[0].Reset()
	assert.Nil(t, msgHandler.array[0].Conn())
	// ss.WritePkg([]byte("hello"), 0)
}

func TestNewWSClient(t *testing.T) {
	var (
		server           Server
		serverMsgHandler MessageHandler
	)
	addr := "127.0.0.1:65000"
	path := "/hello"
	func() {
		server = NewWSServer(
			WithLocalAddress(addr),
			WithWebsocketServerPath(path),
		)
		newServerSession := func(session Session) error {
			return newSessionCallback(session, &serverMsgHandler)
		}
		go server.RunEventLoop(newServerSession)
	}()
	time.Sleep(1e9)

	client := NewWSClient(
		WithServerAddress("ws://"+addr+path),
		WithConnectionNumber(1),
	)

	var msgHandler MessageHandler
	cb := func(session Session) error {
		return newSessionCallback(session, &msgHandler)
	}

	client.RunEventLoop(cb)
	time.Sleep(1e9)

	assert.Equal(t, 1, msgHandler.SessionNumber())
	ss := msgHandler.array[0]
	ss.SetCompressType(CompressNone)
	conn := ss.(*session).Connection.(*gettyWSConn)
	assert.True(t, conn.compress == CompressNone)
	err := conn.handlePing("hello")
	assert.Nil(t, err)
	l, err := conn.Send("hello")
	assert.NotNil(t, err)
	assert.True(t, l == 0)
	beforeWriteBytes := conn.writeBytes
	_, err = conn.Send([]byte("hello"))
	assert.Nil(t, err)
	beforeWriteBytes.Add(5)
	assert.Equal(t, beforeWriteBytes, conn.writeBytes)
	beforeWritePkgNum := conn.writePkgNum
	l, err = ss.WriteBytes([]byte("hello"))
	assert.Nil(t, err)
	assert.True(t, l == 5)
	beforeWritePkgNum.Add(1)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)
	l, err = ss.WriteBytesArray([]byte("hello"), []byte("hello"))
	assert.Nil(t, err)
	assert.True(t, l == 10)
	beforeWritePkgNum.Add(2)
	assert.Equal(t, beforeWritePkgNum, conn.writePkgNum)
	err = conn.writePing()
	assert.Nil(t, err)

	done := make(chan int)
	conn.conn.SetCloseHandler(func(code int, text string) error {
		defer func() {
			done <- code
			close(done)
		}()
		message := websocket.FormatCloseMessage(code, "")
		_ = conn.conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(1e9))
		return nil
	})
	serverSession := serverMsgHandler.array[0]
	serverSession.Close()
	select {
	case code := <-done:
		assert.True(t, code == websocket.CloseNormalClosure)
	case <-time.After(5e9):
		assert.True(t, false)
	}
	assert.True(t, serverSession.IsClosed())

	ss.SetReader(nil)
	assert.Nil(t, ss.(*session).reader)
	ss.SetWriter(nil)
	assert.Nil(t, ss.(*session).writer)
	assert.Nil(t, ss.(*session).GetAttribute("hello"))

	client.Close()
	assert.True(t, client.IsClosed())
	server.Close()
	assert.True(t, server.IsClosed())
}

var (
	WssServerCRT = []byte(`-----BEGIN CERTIFICATE-----
MIICHjCCAYegAwIBAgIQKpKqamBqmZ0hfp8sYb4uNDANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMCAXDTcwMDEwMTAwMDAwMFoYDzIwODQwMTI5MTYw
MDAwWjASMRAwDgYDVQQKEwdBY21lIENvMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCB
iQKBgQC5Nxsk6WjeaYazRYiGxHZ5G3FXSlSjV7lZeebItdEPzO8kVPIGCSTy/M5X
Nnpp3uVDFXQub0/O5t9Y6wcuqpUGMOV+XL7MZqSZlodXm0XhNYzCAjZ+URNjTHGP
NXIqdDEG5Ba8SXMOfY6H97+QxugZoAMFZ+N83ggr12IYNO/FbQIDAQABo3MwcTAO
BgNVHQ8BAf8EBAMCAqQwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDwYDVR0TAQH/BAUw
AwEB/zA5BgNVHREEMjAwgglsb2NhbGhvc3SCC2V4YW1wbGUuY29thwR/AAABhxAA
AAAAAAAAAAAAAAAAAAABMA0GCSqGSIb3DQEBCwUAA4GBAE5dr9q7ORmKZ7yZqeSL
305armc13A7UxffUajeJFujpl2jOqnb5PuKJ7fn5HQKGB0qSq3IHsFua2WONXcTW
Vn4gS0k50IaDpW+yl+ArIo0QwbjPIAcFysX10p9dVO7A1uEpHbRDzefem6r9uVGk
i7dOLEoC8hkfk6nJsNEIEqu6
-----END CERTIFICATE-----`)
	WssServerCRTFile = "/tmp/server.crt"
	WssServerKEY     = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQC5Nxsk6WjeaYazRYiGxHZ5G3FXSlSjV7lZeebItdEPzO8kVPIG
CSTy/M5XNnpp3uVDFXQub0/O5t9Y6wcuqpUGMOV+XL7MZqSZlodXm0XhNYzCAjZ+
URNjTHGPNXIqdDEG5Ba8SXMOfY6H97+QxugZoAMFZ+N83ggr12IYNO/FbQIDAQAB
AoGBAJgvuXQY/fxSxUWkysvBvn9Al17cSrN0r23gBkvBaakMASvfSIbBGMU4COwM
bYV0ivkWNcK539/oQHk1lU85Bv0K9V9wtuFrYW0mN3TU6jnl6eEnzW5oy0Z9TwyY
wuGQOSXGr/aDVu8Wr7eOmSvn6j8rWO2dSMHCllJnSBoqQ1aZAkEA5YQspoMhUaq+
kC53GTgMhotnmK3fWfWKrlLf0spsaNl99W3+plwqxnJbye+5uEutRR1PWSWCCKq5
bN9veOXViwJBAM6WS5aeKO/JX09O0Ang9Y0+atMKO0YjX6fNFE2UJ5Ewzyr4DMZK
TmBpyzm4x/GhV9ukqcDcd3dNlUOtgRqY3+cCQQDCGmssk1+dUpqBE1rT8CvfqYv+
eqWWzerwDNSPz3OppK4630Bqby4Z0GNCP8RAUXgDKIuPqAH11HSm17vNcgqLAkA8
8FCzyUvCD+CxgEoV3+oPFA5m2mnJsr2QvgnzKHTTe1ZhEnKSO3ELN6nfCQbR3AoS
nGwGnAIRiy0wnYmr0tSZAkEAsWFm/D7sTQhX4Qnh15ZDdUn1WSWjBZevUtJnQcpx
TjihZq2sd3uK/XrzG+w7B+cPZlrZtQ94sDSVQwWl/sxB4A==
-----END RSA PRIVATE KEY-----`)
	WssServerKEYFile = "/tmp/server.key"
	WssClientCRT     = []byte(`-----BEGIN CERTIFICATE-----
MIICHjCCAYegAwIBAgIQKpKqamBqmZ0hfp8sYb4uNDANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMCAXDTcwMDEwMTAwMDAwMFoYDzIwODQwMTI5MTYw
MDAwWjASMRAwDgYDVQQKEwdBY21lIENvMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCB
iQKBgQC5Nxsk6WjeaYazRYiGxHZ5G3FXSlSjV7lZeebItdEPzO8kVPIGCSTy/M5X
Nnpp3uVDFXQub0/O5t9Y6wcuqpUGMOV+XL7MZqSZlodXm0XhNYzCAjZ+URNjTHGP
NXIqdDEG5Ba8SXMOfY6H97+QxugZoAMFZ+N83ggr12IYNO/FbQIDAQABo3MwcTAO
BgNVHQ8BAf8EBAMCAqQwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDwYDVR0TAQH/BAUw
AwEB/zA5BgNVHREEMjAwgglsb2NhbGhvc3SCC2V4YW1wbGUuY29thwR/AAABhxAA
AAAAAAAAAAAAAAAAAAABMA0GCSqGSIb3DQEBCwUAA4GBAE5dr9q7ORmKZ7yZqeSL
305armc13A7UxffUajeJFujpl2jOqnb5PuKJ7fn5HQKGB0qSq3IHsFua2WONXcTW
Vn4gS0k50IaDpW+yl+ArIo0QwbjPIAcFysX10p9dVO7A1uEpHbRDzefem6r9uVGk
i7dOLEoC8hkfk6nJsNEIEqu6
-----END CERTIFICATE-----`)
	WssClientCRTFile = "/tmp/client.crt"
)

func DownloadFile(filepath string, content []byte) error {
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()

	// Write the body to file
	_, err = out.Write(content)
	return err
}

func TestNewWSSClient(t *testing.T) {
	var (
		err              error
		server           Server
		serverMsgHandler MessageHandler
	)

	_ = os.Remove(WssServerCRTFile)
	err = DownloadFile(WssServerCRTFile, WssServerCRT)
	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(WssServerCRTFile)
	}()

	_ = os.Remove(WssServerKEYFile)
	err = DownloadFile(WssServerKEYFile, WssServerKEY)
	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(WssServerKEYFile)
	}()

	_ = os.Remove(WssClientCRTFile)
	err = DownloadFile(WssClientCRTFile, WssClientCRT)
	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(WssClientCRTFile)
	}()

	addr := "127.0.0.1:63450"
	path := "/hello"
	func() {
		server = NewWSSServer(
			WithLocalAddress(addr),
			WithWebsocketServerPath(path),
			WithWebsocketServerCert(WssServerCRTFile),
			WithWebsocketServerPrivateKey(WssServerKEYFile),
		)
		newServerSession := func(session Session) error {
			return newSessionCallback(session, &serverMsgHandler)
		}
		go server.RunEventLoop(newServerSession)
	}()
	time.Sleep(1e9)

	client := NewWSSClient(
		WithServerAddress("wss://"+addr+path),
		WithConnectionNumber(1),
		WithRootCertificateFile(WssClientCRTFile),
	)

	var msgHandler MessageHandler
	cb := func(session Session) error {
		return newSessionCallback(session, &msgHandler)
	}

	client.RunEventLoop(cb)
	time.Sleep(1e9)

	assert.Equal(t, 1, msgHandler.SessionNumber())
	client.Close()
	assert.True(t, client.IsClosed())
	assert.False(t, server.IsClosed())
	// time.Sleep(1000e9)
	// server.Close()
	// assert.True(t, server.IsClosed())
}
