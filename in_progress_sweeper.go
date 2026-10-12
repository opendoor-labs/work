package work

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	inProgressSweepPeriod     = time.Minute
	inProgressSweepJitterSecs = 15
)

// inProgressJobs records the in-progress payloads that this pool's workers hold, so the sweeper
// can tell a running job from one that Redis moved to in-progress without the worker knowing.
type inProgressJobs struct {
	mu      sync.Mutex
	running map[string]struct{}
	unacked map[string]unackedJob
}

// unackedJob is a job that finished but whose completion Redis did not confirm.
type unackedJob struct {
	job  *Job
	fate terminateOp
}

func newInProgressJobs() *inProgressJobs {
	return &inProgressJobs{
		running: map[string]struct{}{},
		unacked: map[string]unackedJob{},
	}
}

func inProgressEntry(inProgQueue, rawJSON []byte) string {
	return string(inProgQueue) + "\x00" + string(rawJSON)
}

func (p *inProgressJobs) track(inProgQueue, rawJSON []byte) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running[inProgressEntry(inProgQueue, rawJSON)] = struct{}{}
}

func (p *inProgressJobs) finish(job *Job) {
	if p == nil {
		return
	}
	key := inProgressEntry(job.inProgQueue, job.rawJSON)
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.running, key)
	delete(p.unacked, key)
}

func (p *inProgressJobs) markUnacked(job *Job, fate terminateOp) {
	if p == nil {
		return
	}
	key := inProgressEntry(job.inProgQueue, job.rawJSON)
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.running, key)
	p.unacked[key] = unackedJob{job: job, fate: fate}
}

func (p *inProgressJobs) holds(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.running[key]; ok {
		return true
	}
	_, ok := p.unacked[key]
	return ok
}

func (p *inProgressJobs) unackedJobs() []unackedJob {
	p.mu.Lock()
	defer p.mu.Unlock()
	jobs := make([]unackedJob, 0, len(p.unacked))
	for _, u := range p.unacked {
		jobs = append(jobs, u)
	}
	return jobs
}

// inProgressSweeper repairs a live pool's in-progress lists. A slot leaks when Redis applies the
// fetch script but the reply times out, or when the completion for a finished job fails. The dead
// pool reaper never sees these slots because the pool is still alive.
type inProgressSweeper struct {
	namespace string
	poolID    string
	pool      *redis.Pool
	jobNames  []string
	jobs      *inProgressJobs
	period    time.Duration

	// Payloads that no worker held at the previous sweep. A payload is requeued only when it is
	// still unheld at the next sweep, so a fetch that is about to call track is not requeued.
	suspects map[string]struct{}

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

var requeueOrphanedJobScript = redis.NewScript(4, redisLuaRequeueOrphanedJob)

func newInProgressSweeper(namespace, poolID string, pool *redis.Pool, jobNames []string, jobs *inProgressJobs) *inProgressSweeper {
	return &inProgressSweeper{
		namespace:        namespace,
		poolID:           poolID,
		pool:             pool,
		jobNames:         jobNames,
		jobs:             jobs,
		period:           inProgressSweepPeriod,
		suspects:         map[string]struct{}{},
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (s *inProgressSweeper) start() {
	go s.loop()
}

func (s *inProgressSweeper) stop() {
	s.stopChan <- struct{}{}
	<-s.doneStoppingChan
}

func (s *inProgressSweeper) loop() {
	timer := time.NewTimer(s.period)
	defer timer.Stop()

	for {
		select {
		case <-s.stopChan:
			s.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			timer.Reset(s.period + time.Duration(rand.Intn(inProgressSweepJitterSecs))*time.Second)
			if err := s.sweep(); err != nil {
				logError("in_progress_sweeper.sweep", err)
			}
		}
	}
}

func (s *inProgressSweeper) sweep() error {
	return errors.Join(s.ackUnacked(), s.requeueOrphans())
}

// ackUnacked retries the completion of finished jobs. It returns the first error and keeps the
// remaining jobs for the next sweep.
func (s *inProgressSweeper) ackUnacked() error {
	for _, u := range s.jobs.unackedJobs() {
		if err := completeInProgressJob(s.pool, s.namespace, s.poolID, u.job, u.fate); err != nil {
			return err
		}
		s.jobs.finish(u.job)
	}
	return nil
}

func (s *inProgressSweeper) requeueOrphans() (err error) {
	conn := s.pool.Get()
	defer conn.Close()

	nextSuspects := map[string]struct{}{}
	defer func() {
		if err != nil {
			for key := range s.suspects {
				nextSuspects[key] = struct{}{}
			}
		}
		s.suspects = nextSuspects
	}()

	for _, jobName := range s.jobNames {
		inProgQueue := redisKeyJobsInProgress(s.namespace, s.poolID, jobName)
		payloads, err := redis.ByteSlices(conn.Do("LRANGE", inProgQueue, 0, -1))
		if err != nil {
			return err
		}
		for _, rawJSON := range payloads {
			key := inProgressEntry([]byte(inProgQueue), rawJSON)
			if s.jobs.holds(key) {
				continue
			}
			if _, seen := s.suspects[key]; !seen {
				nextSuspects[key] = struct{}{}
				continue
			}
			_, err := requeueOrphanedJobScript.Do(conn,
				inProgQueue,
				redisKeyJobs(s.namespace, jobName),
				redisKeyJobsLock(s.namespace, jobName),
				redisKeyJobsLockInfo(s.namespace, jobName),
				rawJSON,
				s.poolID,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
