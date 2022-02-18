package stats

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type DefaultTSMetrics struct{}

func (d DefaultTSMetrics) Get() []string {
	return []string{
		"Instances Opened",
		"My Phase 1 Preempted",
		"My Phase 2 Preempted",
		"Instances with Noops Proposed",
		"Instances with Client Values Proposed",
		"Instances Previous Value Proposed",
		"Requeued Client Values",
		"Instances Executed"}
}

type TimeseriesStats struct {
	register    map[string]int32
	orderedKeys []string
	statsFile   *os.File
	C           chan struct{}
	tick        time.Duration
	close       chan struct{}
}

func TimeseriesStatsNew(initalRegisters []string, loc string, tick time.Duration) TimeseriesStats {
	statsFile, _ := os.Create(loc)
	register := make(map[string]int32)
	for i := 0; i < len(initalRegisters); i++ {
		register[initalRegisters[i]] = 0
	}

	return TimeseriesStats{
		register:    register,
		statsFile:   statsFile,
		orderedKeys: initalRegisters,
		C:           make(chan struct{}),
		tick:        tick,
		close:       make(chan struct{}),
	}
}

func (s *TimeseriesStats) Reset() {
	for k, _ := range s.register {
		s.register[k] = 0
	}
}

func (s *TimeseriesStats) Update(stat string, count int32) {
	s.register[stat] = s.register[stat] + count
}

func (s *TimeseriesStats) Get(stat string) int32 {
	return s.register[stat]
}

func (s *TimeseriesStats) Print() {
	str := strings.Builder{}
	for i := 0; i < len(s.orderedKeys); i++ {
		k := s.orderedKeys[i]
		v := s.register[k]
		str.WriteString(fmt.Sprintf("%s : %d ", k, v))
	}

	str.WriteString("\n")
	s.statsFile.WriteString(time.Now().Format("2006/01/02 15:04:05 .000 ") + str.String())
}

func (s *TimeseriesStats) PrintAndReset() {
	s.Print()
	s.Reset()
}

func (s *TimeseriesStats) GoClock() {
	go func() {
		loop := true
		for loop {
			time.Sleep(s.tick)
			s.C <- struct{}{}

			select {
			case <-s.close:
				loop = false
			default:
				break
			}
		}
	}()
}

func (s *TimeseriesStats) Close() {
	s.close <- struct{}{}
	s.statsFile.Close()
}
