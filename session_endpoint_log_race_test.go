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
	"fmt"
	"net"
	"sync"
	"testing"
)

// fmtDiscardLogger still formats its arguments (so a %s walk of EndPoint
// is exercised) but does not write them. It is only a test sink.
type fmtDiscardLogger struct{}

func (fmtDiscardLogger) Info(args ...interface{})             { _ = fmt.Sprint(args...) }
func (fmtDiscardLogger) Warn(args ...interface{})             { _ = fmt.Sprint(args...) }
func (fmtDiscardLogger) Error(args ...interface{})            { _ = fmt.Sprint(args...) }
func (fmtDiscardLogger) Debug(args ...interface{})            { _ = fmt.Sprint(args...) }
func (fmtDiscardLogger) Infof(f string, args ...interface{})  { _ = fmt.Sprintf(f, args...) }
func (fmtDiscardLogger) Warnf(f string, args ...interface{})  { _ = fmt.Sprintf(f, args...) }
func (fmtDiscardLogger) Errorf(f string, args ...interface{}) { _ = fmt.Sprintf(f, args...) }
func (fmtDiscardLogger) Debugf(f string, args ...interface{}) { _ = fmt.Sprintf(f, args...) }

func mutateClientMap(c *client, s Session, n int) {
	for i := 0; i < n; i++ {
		c.Lock()
		if c.ssMap == nil {
			c.ssMap = make(map[Session]struct{})
		}
		if s != nil {
			c.ssMap[s] = struct{}{}
		}
		c.ssMap = nil
		c.Unlock()
	}
	c.stop()
}

// TestAddTaskClosedSessionLogRacesWithClientStop reproduces the production
// path: a closed session's addTask worker logs the EndPoint while
// client.stop() writes ssMap under the client mutex. fmt's default %s walk
// of *client copies that mutex and map.
func TestAddTaskClosedSessionLogRacesWithClientStop(t *testing.T) {
	prev := GetLogger()
	t.Cleanup(func() { SetLogger(prev) })
	SetLogger(fmtDiscardLogger{})

	c := NewTCPClient(
		WithServerAddress("127.0.0.1:1"),
		WithConnectionNumber(1),
	).(*client)

	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	ss := newTCPSession(left, c).(*session)
	ss.stop() // IsClosed, so addTask only logs and returns

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20000; i++ {
			ss.addTask(struct{}{})
		}
	}()
	go func() {
		defer wg.Done()
		mutateClientMap(c, ss, 20000)
	}()
	wg.Wait()
}
