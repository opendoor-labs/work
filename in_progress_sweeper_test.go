package work

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedInProgressJob(t *testing.T, pool *redis.Pool, ns, poolID, jobName, rawJSON string) {
	t.Helper()
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("LPUSH", redisKeyJobsInProgress(ns, poolID, jobName), rawJSON)
	require.NoError(t, err)
	_, err = conn.Do("INCR", redisKeyJobsLock(ns, jobName))
	require.NoError(t, err)
	_, err = conn.Do("HINCRBY", redisKeyJobsLockInfo(ns, jobName), poolID, 1)
	require.NoError(t, err)
}

func TestInProgressSweeperRequeuesUnheldJobOnSecondSweep(t *testing.T) {
	pool := newTestPool(t)
	ns, poolID, jobName := "work", "pool1", "persist"
	seedInProgressJob(t, pool, ns, poolID, jobName, `{"name":"persist","id":"orphan"}`)
	sweeper := newInProgressSweeper(ns, poolID, pool, []string{jobName}, newInProgressJobs())

	require.NoError(t, sweeper.sweep())
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobsInProgress(ns, poolID, jobName)))
	assert.EqualValues(t, 1, getInt64(pool, redisKeyJobsLock(ns, jobName)))

	require.NoError(t, sweeper.sweep())
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, poolID, jobName)))
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobs(ns, jobName)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, jobName)))
	assert.EqualValues(t, 0, hgetInt64(pool, redisKeyJobsLockInfo(ns, jobName), poolID))
}

func TestInProgressSweeperLeavesHeldJobs(t *testing.T) {
	pool := newTestPool(t)
	ns, poolID, jobName := "work", "pool1", "persist"
	held := `{"name":"persist","id":"held"}`
	late := `{"name":"persist","id":"late"}`
	seedInProgressJob(t, pool, ns, poolID, jobName, held)
	seedInProgressJob(t, pool, ns, poolID, jobName, late)
	jobs := newInProgressJobs()
	inProgQueue := []byte(redisKeyJobsInProgress(ns, poolID, jobName))
	jobs.track(inProgQueue, []byte(held))
	sweeper := newInProgressSweeper(ns, poolID, pool, []string{jobName}, jobs)

	require.NoError(t, sweeper.sweep())
	jobs.track(inProgQueue, []byte(late))
	require.NoError(t, sweeper.sweep())
	require.NoError(t, sweeper.sweep())

	assert.EqualValues(t, 2, listSize(pool, redisKeyJobsInProgress(ns, poolID, jobName)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, jobName)))
	assert.EqualValues(t, 2, getInt64(pool, redisKeyJobsLock(ns, jobName)))
}

func TestInProgressSweeperAcksUnackedCompletionOnce(t *testing.T) {
	pool := newTestPool(t)
	ns, poolID, jobName := "work", "pool1", "persist"
	rawJSON := `{"name":"persist","id":"done"}`
	seedInProgressJob(t, pool, ns, poolID, jobName, rawJSON)
	seedInProgressJob(t, pool, ns, poolID, jobName, `{"name":"persist","id":"running"}`)
	jobs := newInProgressJobs()
	inProgQueue := []byte(redisKeyJobsInProgress(ns, poolID, jobName))
	jobs.track(inProgQueue, []byte(`{"name":"persist","id":"running"}`))
	job := &Job{Name: jobName, rawJSON: []byte(rawJSON), inProgQueue: inProgQueue}
	jobs.markUnacked(job, terminateOp{zsetKey: redisKeyRetry(ns), score: 100, rawJSON: []byte(rawJSON)})
	sweeper := newInProgressSweeper(ns, poolID, pool, []string{jobName}, jobs)

	require.NoError(t, sweeper.sweep())
	require.NoError(t, completeInProgressJob(pool, ns, poolID, job, terminateOnly))
	require.NoError(t, sweeper.sweep())

	assert.EqualValues(t, 1, listSize(pool, redisKeyJobsInProgress(ns, poolID, jobName)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, jobName)))
	assert.EqualValues(t, 1, zsetSize(pool, redisKeyRetry(ns)))
	assert.EqualValues(t, 1, getInt64(pool, redisKeyJobsLock(ns, jobName)))
	assert.EqualValues(t, 1, hgetInt64(pool, redisKeyJobsLockInfo(ns, jobName), poolID))
	assert.Empty(t, jobs.unackedJobs())
}

func TestInProgressSweeperKeepsUnackedCompletionWhenRedisRejectsIt(t *testing.T) {
	pool := newTestPool(t)
	ns, poolID, jobName := "work", "pool1", "persist"
	rawJSON := `{"name":"persist","id":"done"}`
	inProgQueue := redisKeyJobsInProgress(ns, poolID, jobName)
	seedInProgressJob(t, pool, ns, poolID, jobName, rawJSON)
	conn := pool.Get()
	defer conn.Close()
	// A wrong-type key fails the first command of the script, as an OOM rejection does.
	_, err := conn.Do("SET", inProgQueue, "not-a-list")
	require.NoError(t, err)
	jobs := newInProgressJobs()
	job := &Job{Name: jobName, rawJSON: []byte(rawJSON), inProgQueue: []byte(inProgQueue)}
	jobs.markUnacked(job, terminateOnly)
	sweeper := newInProgressSweeper(ns, poolID, pool, []string{jobName}, jobs)

	assert.Error(t, sweeper.sweep())
	assert.Len(t, jobs.unackedJobs(), 1)
	assert.EqualValues(t, 1, getInt64(pool, redisKeyJobsLock(ns, jobName)))

	_, err = conn.Do("DEL", inProgQueue)
	require.NoError(t, err)
	_, err = conn.Do("LPUSH", inProgQueue, rawJSON)
	require.NoError(t, err)
	require.NoError(t, sweeper.sweep())
	assert.Empty(t, jobs.unackedJobs())
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, poolID, jobName)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, jobName)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, jobName)))
}

func TestInProgressSweeperRecoversJobWhoseFetchReplyWasLost(t *testing.T) {
	pool := newTestPool(t)
	ns, jobName := "work", "persist"
	ran := 0
	jobTypes := map[string]*jobType{jobName: {
		Name: jobName, JobOptions: JobOptions{Priority: 1, MaxConcurrency: 1}, IsGeneric: true,
		GenericHandler: func(*Job) error { ran++; return nil },
	}}
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", redisKeyJobsConcurrency(ns, jobName), 1)
	require.NoError(t, err)
	_, err = NewEnqueuer(ns, pool).Enqueue(jobName, nil)
	require.NoError(t, err)

	lostReply := newWorker(ns, "pool1", pool, tstCtxType, nil, jobTypes, nil)
	job, err := lostReply.fetchJob()
	require.NoError(t, err)
	require.NotNil(t, job)

	jobs := newInProgressJobs()
	w := newWorker(ns, "pool1", pool, tstCtxType, nil, jobTypes, nil)
	w.inProgress = jobs
	fetched, err := w.fetchJob()
	require.NoError(t, err)
	assert.Nil(t, fetched, "the leaked slot blocks the queue")

	sweeper := newInProgressSweeper(ns, "pool1", pool, []string{jobName}, jobs)
	require.NoError(t, sweeper.sweep())
	require.NoError(t, sweeper.sweep())

	w.start()
	w.drain()
	w.stop()
	assert.Equal(t, 1, ran)
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, "pool1", jobName)))
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, jobName)))
	assert.EqualValues(t, 0, hgetInt64(pool, redisKeyJobsLockInfo(ns, jobName), "pool1"))
	assert.Empty(t, jobs.unackedJobs())
}
