package unikontainers

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

const startupProfileEnv = "URUNC_PROFILE_STARTUP"

type phaseTimer struct {
	phase  string
	start  time.Time
	fields logrus.Fields
}

func startupProfilingEnabled() bool {
	return os.Getenv(startupProfileEnv) == "1"
}

func startPhaseTimer(phase string, fields logrus.Fields) *phaseTimer {
	if !startupProfilingEnabled() {
		return nil
	}
	return &phaseTimer{
		phase:  phase,
		start:  time.Now(),
		fields: fields,
	}
}

func (t *phaseTimer) done(err error) {
	if t == nil {
		return
	}
	logPhaseDuration(t.phase, time.Since(t.start), t.fields, err)
}

func logPhaseDuration(phase string, dur time.Duration, fields logrus.Fields, err error) {
	if !startupProfilingEnabled() {
		return
	}
	merged := logrus.Fields{
		"phase":       phase,
		"duration_ms": dur.Milliseconds(),
	}
	for k, v := range fields {
		merged[k] = v
	}
	entry := uniklog.WithFields(merged)
	if err != nil {
		entry.WithError(err).Warn("startup profile phase completed with error")
		return
	}
	entry.Info("startup profile phase completed")
}
