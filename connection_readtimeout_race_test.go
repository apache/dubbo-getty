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
	"testing"
)

// TestGettyConnReadTimeoutRacesWithIncReadPkgNum is a minimal baseline for
// the value-receiver copy of gettyConn atomics. session.stop() calls
// ReadTimeout() while an addTask worker may IncReadPkgNum() on the same conn.
func TestGettyConnReadTimeoutRacesWithIncReadPkgNum(t *testing.T) {
	c := &gettyConn{}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200000; i++ {
			c.IncReadPkgNum()
			c.IncWritePkgNum()
		}
		close(done)
	}()
	for i := 0; i < 200000; i++ {
		_ = c.ReadTimeout()
		_ = c.WriteTimeout()
	}
	<-done
}
