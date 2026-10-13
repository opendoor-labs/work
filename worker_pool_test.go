package work

import (
	"bytes"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

type tstCtx struct {
	a int
	bytes.Buffer
}

func (c *tstCtx) record(s string) {
	_, _ = c.WriteString(s)
}

var tstCtxType = reflect.TypeOf(tstCtx{})

func TestWorkerPoolHandlerValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *Job) error { return nil }, true},
		{func(c *tstCtx, j *Job) error { return nil }, true},
		{func(c *tstCtx, j *Job) {}, false},
		{func(c *tstCtx, j *Job) string { return "" }, false},
		{func(c *tstCtx, j *Job) (error, string) { return nil, "" }, false},
		{func(c *tstCtx) error { return nil }, false},
		{func(c tstCtx, j *Job) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *tstCtx, j *Job, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidHandlerType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolMiddlewareValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *Job, n NextMiddlewareFunc) error { return nil }, true},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) error { return nil }, true},
		{func(c *tstCtx, j *Job) error { return nil }, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) {}, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) string { return "" }, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc) (error, string) { return nil, "" }, false},
		{func(c *tstCtx, n NextMiddlewareFunc) error { return nil }, false},
		{func(c tstCtx, j *Job, n NextMiddlewareFunc) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *tstCtx, j *Job, wat string) error { return nil }, false},
		{func(c *tstCtx, j *Job, n NextMiddlewareFunc, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidMiddlewareType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolStartStop(t *testing.T) {
	pool := newTestPool(t)
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, pool)
	wp.Start()
	wp.Start()
	wp.Stop()
	wp.Stop()
	wp.Start()
	wp.Stop()
}

func TestWorkerPoolStarted(t *testing.T) {
	pool := newTestPool(t)
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, pool)

	assert.False(t, wp.Started())
	wp.Start()
	assert.True(t, wp.Started())
	wp.Stop()
	assert.False(t, wp.Started())
}

func TestWorkerPoolValidations(t *testing.T) {
	pool := newTestPool(t)
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, pool)

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your middleware function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using bad middleware")
			}
		}()

		wp.Middleware(TestWorkerPoolValidations)
	}()

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your handler function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using a bad handler")
			}
		}()

		wp.Job("wat", TestWorkerPoolValidations)
	}()
}

func TestWorkersPoolRunSingleThreaded(t *testing.T) {
	pool := newTestPool(t)
	ns := "work"
	job1 := "job1"
	numJobs, concurrency, sleepTime := 5, 5, 2
	wp := setupTestWorkerPool(pool, ns, job1, concurrency, JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}

	// make sure we've enough jobs queued up to make an interesting test
	jobsQueued := listSize(pool, redisKeyJobs(ns, job1))
	assert.True(t, jobsQueued >= 3, "should be at least 3 jobs queued up, but only found %v", jobsQueued)

	// now make sure the during the duration of job execution there is never > 1 job in flight
	start := time.Now()
	totalRuntime := time.Duration(sleepTime*numJobs) * time.Millisecond
	time.Sleep(10 * time.Millisecond)
	for time.Since(start) < totalRuntime {
		// jobs in progress, lock count for the job and lock info for the pool should never exceed 1
		jobsInProgress := listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1))
		assert.True(t, jobsInProgress <= 1, "jobsInProgress should never exceed 1: actual=%d", jobsInProgress)

		jobLockCount := getInt64(pool, redisKeyJobsLock(ns, job1))
		assert.True(t, jobLockCount <= 1, "global lock count for job should never exceed 1, got: %v", jobLockCount)
		wpLockCount := hgetInt64(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID)
		assert.True(t, wpLockCount <= 1, "lock count for the worker pool should never exceed 1: actual=%v", wpLockCount)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
	assert.False(t, hexists(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

func TestWorkerPoolPauseSingleThreadedJobs(t *testing.T) {
	pool := newTestPool(t)
	ns, job1 := "work", "job1"
	numJobs, concurrency, sleepTime := 5, 5, 2
	wp := setupTestWorkerPool(pool, ns, job1, concurrency, JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}
	// provide time for jobs to process
	time.Sleep(10 * time.Millisecond)

	// pause work, provide time for outstanding jobs to finish and queue up another job
	pauseJobs(ns, job1, pool)
	time.Sleep(2 * time.Millisecond)
	_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
	assert.Nil(t, err)

	// check that we still have some jobs to process
	assert.True(t, listSize(pool, redisKeyJobs(ns, job1)) >= 1)

	// now make sure no jobs get started until we unpause
	start := time.Now()
	totalRuntime := time.Duration(sleepTime*numJobs) * time.Millisecond
	for time.Since(start) < totalRuntime {
		assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
		// lock count for the job and lock info for the pool should both be at 1 while job is running
		assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
		assert.EqualValues(t, 0, hgetInt64(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}

	// unpause work and get past the backoff time
	unpauseJobs(ns, job1, pool)
	time.Sleep(10 * time.Millisecond)

	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
	assert.False(t, hexists(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

func TestWorkerPoolMaxLocalConcurrency(t *testing.T) {
	pool := newTestPool(t)
	ns := "work"
	job1 := "job1"
	numJobs := 10
	concurrency := 6
	sleepTime := 50 // ms per job

	deleteQueue(pool, ns, job1)
	deleteRetryAndDead(pool, ns)
	deletePausedAndLockedKeys(ns, job1, pool)

	var current atomic.Int64
	var peak atomic.Int64

	wp := NewWorkerPool(TestContext{}, uint(concurrency), ns, pool)
	wp.JobWithOptions(job1, JobOptions{Priority: 1, MaxLocalConcurrency: 2}, func(job *Job) error {
		c := current.Add(1)
		for {
			old := peak.Load()
			if c <= old || peak.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		current.Add(-1)
		return nil
	})
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	wp.Start()

	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"i": i})
		assert.NoError(t, err)
	}

	// Wait for all jobs to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		remaining := listSize(pool, redisKeyJobs(ns, job1))
		inProgress := listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1))
		if remaining == 0 && inProgress == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	wp.Drain()
	wp.Stop()

	assert.LessOrEqual(t, peak.Load(), int64(2), "peak concurrency should not exceed MaxLocalConcurrency=2")
	assert.Greater(t, peak.Load(), int64(0), "at least one job should have run")
}

func TestWorkerPoolNoMaxLocalConcurrency(t *testing.T) {
	pool := newTestPool(t)
	ns := "work"
	job1 := "job1"
	numJobs := 6
	concurrency := 6
	sleepTime := 100 // ms per job

	deleteQueue(pool, ns, job1)
	deleteRetryAndDead(pool, ns)
	deletePausedAndLockedKeys(ns, job1, pool)

	var current atomic.Int64
	var peak atomic.Int64

	wp := NewWorkerPool(TestContext{}, uint(concurrency), ns, pool)
	wp.JobWithOptions(job1, JobOptions{Priority: 1}, func(job *Job) error {
		c := current.Add(1)
		for {
			old := peak.Load()
			if c <= old || peak.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		current.Add(-1)
		return nil
	})
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	wp.Start()

	enqueuer := NewEnqueuer(ns, pool)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"i": i})
		assert.NoError(t, err)
	}

	// Wait for all jobs to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		remaining := listSize(pool, redisKeyJobs(ns, job1))
		inProgress := listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1))
		if remaining == 0 && inProgress == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	wp.Drain()
	wp.Stop()

	assert.Greater(t, peak.Load(), int64(2), "without MaxLocalConcurrency, peak should exceed 2 with 6 workers")
}

// Test Helpers
func (t *TestContext) SleepyJob(job *Job) error {
	sleepTime := time.Duration(job.ArgInt64("sleep"))
	time.Sleep(sleepTime * time.Millisecond)
	return nil
}

func setupTestWorkerPool(pool *redis.Pool, namespace, jobName string, concurrency int, jobOpts JobOptions) *WorkerPool {
	deleteQueue(pool, namespace, jobName)
	deleteRetryAndDead(pool, namespace)
	deletePausedAndLockedKeys(namespace, jobName, pool)

	wp := NewWorkerPool(TestContext{}, uint(concurrency), namespace, pool)
	wp.JobWithOptions(jobName, jobOpts, (*TestContext).SleepyJob)
	// reset the backoff times to help with testing
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	return wp
}

func TestWorkerPoolStopKeepsRegistrationWhenCleanupFails(t *testing.T) {
	pool := newTestPool(t)
	ns, job1 := "work", "job1"
	wp := setupTestWorkerPool(pool, ns, job1, 1, JobOptions{Priority: 1})
	_, err := NewEnqueuer(ns, pool).Enqueue(job1, Q{"sleep": 0})
	assert.NoError(t, err)
	wp.Start()
	wp.Drain()

	conn := pool.Get()
	defer conn.Close()
	_, err = conn.Do("SET", redisKeyJobsLock(ns, job1), "not-a-number")
	assert.NoError(t, err)
	wp.Stop()

	isMember, err := redis.Bool(conn.Do("SISMEMBER", redisKeyWorkerPools(ns), wp.workerPoolID))
	assert.NoError(t, err)
	assert.True(t, isMember)
	exists, err := redis.Bool(conn.Do("EXISTS", redisKeyHeartbeat(ns, wp.workerPoolID)))
	assert.NoError(t, err)
	assert.True(t, exists)

	_, err = conn.Do("SET", redisKeyJobsLock(ns, job1), 0)
	assert.NoError(t, err)
	reaper := newDeadPoolReaper(ns, pool, []string{job1})
	reaper.deadTime = -time.Minute
	assert.NoError(t, reaper.reap())
	isMember, err = redis.Bool(conn.Do("SISMEMBER", redisKeyWorkerPools(ns), wp.workerPoolID))
	assert.NoError(t, err)
	assert.False(t, isMember)
	assert.False(t, hexists(pool, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

func TestWorkerPoolStopDeregistersAfterCleanup(t *testing.T) {
	pool := newTestPool(t)
	ns, job1 := "work", "job1"
	wp := setupTestWorkerPool(pool, ns, job1, 1, JobOptions{Priority: 1})
	wp.Start()
	wp.Drain()
	wp.Stop()

	conn := pool.Get()
	defer conn.Close()
	isMember, err := redis.Bool(conn.Do("SISMEMBER", redisKeyWorkerPools(ns), wp.workerPoolID))
	assert.NoError(t, err)
	assert.False(t, isMember)
	exists, err := redis.Bool(conn.Do("EXISTS", redisKeyHeartbeat(ns, wp.workerPoolID)))
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestWorkerPoolDoesNotFetchBeforeFirstHeartbeat(t *testing.T) {
	pool := newTestPool(t)
	ns, job1 := "work", "job1"
	wp := setupTestWorkerPool(pool, ns, job1, 1, JobOptions{Priority: 1})
	_, err := NewEnqueuer(ns, pool).Enqueue(job1, Q{"sleep": 0})
	assert.NoError(t, err)
	conn := pool.Get()
	defer conn.Close()
	_, err = conn.Do("SET", redisKeyWorkerPools(ns), "not-a-set")
	assert.NoError(t, err)

	wp.Start()
	defer wp.Stop()
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobs(ns, job1)))

	_, err = conn.Do("DEL", redisKeyWorkerPools(ns))
	assert.NoError(t, err)
	wp.heartbeater.heartbeat()
	wp.Drain()
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
}

func TestWorkerPoolStopRetriesUnackedCompletion(t *testing.T) {
	pool, server := newTestPoolWithServer(t)
	ns, job1 := "work", "job1"
	wp := setupTestWorkerPool(pool, ns, job1, 1, JobOptions{Priority: 1})
	defer func(backoffs []time.Duration) { stopAckBackoffs = backoffs }(stopAckBackoffs)
	stopAckBackoffs = []time.Duration{0, 100 * time.Millisecond, 100 * time.Millisecond}
	wp.Start()

	rawJSON := `{"name":"job1","id":"finished"}`
	seedInProgressJob(t, pool, ns, wp.workerPoolID, job1, rawJSON)
	inProgQueue := redisKeyJobsInProgress(ns, wp.workerPoolID, job1)
	wp.inProgress.markUnacked(&Job{Name: job1, rawJSON: []byte(rawJSON), inProgQueue: []byte(inProgQueue)}, terminateOnly)

	server.SetError("OOM command not allowed when used memory > 'maxmemory'.")
	go func() {
		time.Sleep(30 * time.Millisecond)
		server.SetError("")
	}()
	wp.Stop()

	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, inProgQueue))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, job1)))
}

func TestWorkerPoolStopForgetsRequeuedPayloads(t *testing.T) {
	pool := newTestPool(t)
	ns, job1 := "work", "job1"
	wp := setupTestWorkerPool(pool, ns, job1, 1, JobOptions{Priority: 1})
	defer func(backoffs []time.Duration) { stopAckBackoffs = backoffs }(stopAckBackoffs)
	stopAckBackoffs = []time.Duration{0}
	wp.Start()

	inProgQueue := redisKeyJobsInProgress(ns, wp.workerPoolID, "other")
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", inProgQueue, "not-a-list")
	assert.NoError(t, err)
	wp.inProgress.markUnacked(&Job{Name: "other", rawJSON: []byte(`{"id":"a"}`), inProgQueue: []byte(inProgQueue)}, terminateOnly)

	wp.Stop()

	assert.Empty(t, wp.inProgress.unackedJobs())
}
