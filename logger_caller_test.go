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
	"net"
	"path/filepath"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestLoggerCallerPointsAtCallSite guards against a forwarding layer
// between getty call sites and the configured logger: a caller-enabled
// logger must record the getty source line, not logger.go.
func TestLoggerCallerPointsAtCallSite(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	prev := GetLogger()
	t.Cleanup(func() { SetLogger(prev) })
	SetLogger(zap.New(core, zap.AddCaller()).Sugar())

	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	c := NewTCPClient(
		WithServerAddress("127.0.0.1:1"),
		WithConnectionNumber(1),
	).(*client)
	ss := newTCPSession(left, c).(*session)
	ss.stop()
	ss.addTask(struct{}{}) // closed session without a task pool logs synchronously

	entries := logs.FilterMessageSnippet("Session is closed").All()
	require.Len(t, entries, 1)
	caller := entries[0].Caller
	require.True(t, caller.Defined)
	assert.Equal(t, "session.go", filepath.Base(caller.File))
}
