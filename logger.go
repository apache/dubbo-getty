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
	"sync/atomic"
)

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger for user who want to customize logger of getty
type Logger interface {
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
	Debug(args ...any)

	Infof(fmt string, args ...any)
	Warnf(fmt string, args ...any)
	Errorf(fmt string, args ...any)
	Debugf(fmt string, args ...any)
}

type LoggerLevel int8

const (
	// LoggerLevelDebug DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	LoggerLevelDebug = LoggerLevel(zapcore.DebugLevel)
	// LoggerLevelInfo InfoLevel is the default logging priority.
	LoggerLevelInfo = LoggerLevel(zapcore.InfoLevel)
	// LoggerLevelWarn WarnLevel logs are more important than Infof, but don't need individual
	// human review.
	LoggerLevelWarn = LoggerLevel(zapcore.WarnLevel)
	// LoggerLevelError ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	LoggerLevelError = LoggerLevel(zapcore.ErrorLevel)
	// LoggerLevelDPanic DPanicLevel logs are particularly important errors. In development the
	// logger panics after writing the message.
	LoggerLevelDPanic = LoggerLevel(zapcore.DPanicLevel)
	// LoggerLevelPanic PanicLevel logs a message, then panics.
	LoggerLevelPanic = LoggerLevel(zapcore.PanicLevel)
	// LoggerLevelFatal FatalLevel logs a message, then calls os.Exit(1).
	LoggerLevelFatal = LoggerLevel(zapcore.FatalLevel)
)

// loggerHolder atomically stores the process logger so SetLogger can
// be called without racing in-flight log calls.
type loggerHolder struct {
	inner atomic.Value // Logger
}

type loggerBox struct {
	logger Logger
}

func (l *loggerHolder) current() Logger {
	v := l.inner.Load()
	if v == nil {
		return nil
	}
	return v.(loggerBox).logger
}

func (l *loggerHolder) Info(args ...any)  { l.current().Info(args...) }
func (l *loggerHolder) Warn(args ...any)  { l.current().Warn(args...) }
func (l *loggerHolder) Error(args ...any) { l.current().Error(args...) }
func (l *loggerHolder) Debug(args ...any) { l.current().Debug(args...) }
func (l *loggerHolder) Infof(fmt string, args ...any) {
	l.current().Infof(fmt, args...)
}
func (l *loggerHolder) Warnf(fmt string, args ...any) {
	l.current().Warnf(fmt, args...)
}
func (l *loggerHolder) Errorf(fmt string, args ...any) {
	l.current().Errorf(fmt, args...)
}
func (l *loggerHolder) Debugf(fmt string, args ...any) {
	l.current().Debugf(fmt, args...)
}

var (
	log       = &loggerHolder{}
	zapLogger *zap.Logger

	zapLoggerConfig        = zap.NewDevelopmentConfig()
	zapLoggerEncoderConfig = zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
)

func init() {
	zapLoggerConfig.EncoderConfig = zapLoggerEncoderConfig
	zapLogger, _ = zapLoggerConfig.Build()
	log.inner.Store(loggerBox{Logger(zapLogger.Sugar())})

	// todo: flushes buffer when redirect log to file.
	// var exitSignal = make(chan os.Signal)
	// signal.Notify(exitSignal, syscall.SIGTERM, syscall.SIGINT)
	// go func() {
	// 	<-exitSignal
	// 	// Sync calls the underlying Core's Sync method, flushing any buffered log
	// 	// entries. Applications should take care to call Sync before exiting.
	// 	err := zapLogger.Sync() // flushes buffer, if any
	// 	if err != nil {
	// 		fmt.Printf("zapLogger sync err: %+v", perrors.WithStack(err))
	// 	}
	// 	os.Exit(0)
	// }()
}

// SetLogger customize yourself logger.
func SetLogger(logger Logger) {
	if logger == nil {
		return
	}
	log.inner.Store(loggerBox{logger})
}

// GetLogger get getty logger
func GetLogger() Logger {
	return log.current()
}

// SetLoggerLevel set logger level
func SetLoggerLevel(level LoggerLevel) error {
	var err error
	zapLoggerConfig.Level = zap.NewAtomicLevelAt(zapcore.Level(level))
	zapLogger, err = zapLoggerConfig.Build()
	if err != nil {
		return err
	}
	log.inner.Store(loggerBox{Logger(zapLogger.Sugar())})
	return nil
}

// SetLoggerCallerDisable disable caller info in production env for performance improve.
// It is highly recommended that you execute this method in a production environment.
func SetLoggerCallerDisable() error {
	var err error
	zapLoggerConfig.Development = false
	zapLoggerConfig.DisableCaller = true
	zapLogger, err = zapLoggerConfig.Build()
	if err != nil {
		return err
	}
	log.inner.Store(loggerBox{Logger(zapLogger.Sugar())})
	return nil
}
