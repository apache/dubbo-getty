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
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestTCPConnSetCompressTypeRacesWithRecvAndSend is a minimal repro of the
// data race between SetCompressType (which replaces reader/writer/compress)
// and a live recv/Send loop. The official TestTCPClient also changes
// compression after RunEventLoop; this test isolates that interleaving
// without a full client/server fixture.
func TestTCPConnSetCompressTypeRacesWithRecvAndSend(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})

	conn := newGettyTCPConn(c1)

	// Keep the pipe flowing so recv/Send are not stuck on a blocked pipe.
	peerDone := make(chan struct{})
	var peerWG sync.WaitGroup
	peerWG.Add(1)
	go func() {
		defer peerWG.Done()
		buf := make([]byte, 256)
		for {
			select {
			case <-peerDone:
				return
			default:
			}
			_ = c2.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			if _, err := c2.Write([]byte("x")); err != nil && err != io.EOF {
				select {
				case <-peerDone:
					return
				default:
				}
			}
			_ = c2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			if _, err := c2.Read(buf); err != nil {
				select {
				case <-peerDone:
					return
				default:
				}
			}
		}
	}()

	const iters = 20000
	types := []CompressType{CompressNone, CompressSnappy, CompressZip}
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		p := make([]byte, 64)
		for i := 0; i < iters; i++ {
			_, _ = conn.recv(p)
		}
	}()
	go func() {
		defer wg.Done()
		msg := []byte("hello")
		for i := 0; i < iters; i++ {
			_, _ = conn.Send(msg)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			conn.SetCompressType(types[i%len(types)])
		}
	}()

	wg.Wait()
	close(peerDone)
	_ = c1.Close()
	_ = c2.Close()
	peerWG.Wait()
}
