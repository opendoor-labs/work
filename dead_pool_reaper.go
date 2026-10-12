package work

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	deadTime          = 10 * time.Second // 2 x heartbeat
	reapPeriod        = 10 * time.Minute
	reapJitterSecs    = 30
	requeueKeysPerJob = 4
)

type deadPoolReaper struct {
	namespace   string
	pool        *redis.Pool
	deadTime    time.Duration
	reapPeriod  time.Duration
	curJobTypes []string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newDeadPoolReaper(namespace string, pool *redis.Pool, curJobTypes []string) *deadPoolReaper {
	return &deadPoolReaper{
		namespace:        namespace,
		pool:             pool,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (r *deadPoolReaper) start() {
	go r.loop()
}

func (r *deadPoolReaper) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *deadPoolReaper) loop() {
	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			// Reap
			if err := r.reap(); err != nil {
				logError("dead_pool_reaper.reap", err)
			}
		}
	}
}

func (r *deadPoolReaper) reap() error {
	// Get dead pools
	deadPoolIDs, err := r.findDeadPools()
	if err != nil {
		return err
	}

	conn := r.pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(r.namespace)

	// Cleanup all dead pools. Each step must succeed before the next one removes the record that
	// lets a later reap retry it.
	for deadPoolID, jobTypes := range deadPoolIDs {
		if err = r.requeueInProgressJobs(deadPoolID, jobTypes); err != nil {
			return err
		}
		if err = r.cleanStaleLockInfo(deadPoolID, jobTypes); err != nil {
			return err
		}
		if _, err = conn.Do("DEL", redisKeyHeartbeat(r.namespace, deadPoolID)); err != nil {
			return err
		}

		// Remove dead pool from worker pools set
		if _, err = conn.Do("SREM", workerPoolsKey, deadPoolID); err != nil {
			return err
		}
	}

	return nil
}

func (r *deadPoolReaper) cleanStaleLockInfo(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * 2
	redisReapLocksScript := redis.NewScript(numKeys, redisLuaReapStaleLocks)
	var scriptArgs = make([]interface{}, 0, numKeys+1) // +1 for argv[1]

	for _, jobType := range jobTypes {
		scriptArgs = append(scriptArgs, redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType))
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()
	if _, err := redisReapLocksScript.Do(conn, scriptArgs...); err != nil {
		return err
	}

	return nil
}

func (r *deadPoolReaper) requeueInProgressJobs(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * requeueKeysPerJob
	redisRequeueScript := redis.NewScript(numKeys, redisLuaReenqueueJob)
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, jobType := range jobTypes {
		// pops from in progress, push into job queue and decrement the queue lock
		scriptArgs = append(scriptArgs, redisKeyJobsInProgress(r.namespace, poolID, jobType), redisKeyJobs(r.namespace, jobType), redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType)) // KEYS[1-4 * N]
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()

	// Keep moving jobs until all queues are empty
	for {
		values, err := redis.Values(redisRequeueScript.Do(conn, scriptArgs...))
		if err == redis.ErrNil {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

func (r *deadPoolReaper) findDeadPools() (map[string][]string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(r.namespace)

	workerPoolIDs, err := redis.Strings(conn.Do("SMEMBERS", workerPoolsKey))
	if err != nil {
		return nil, err
	}
	unregistered, err := r.findUnregisteredLockHolders(conn, workerPoolIDs)
	if err != nil {
		return nil, err
	}

	deadPools := map[string][]string{}
	for _, workerPoolID := range append(workerPoolIDs, unregistered...) {
		dead, jobTypes, err := r.checkPool(conn, workerPoolID)
		if err != nil {
			return nil, err
		}
		if dead {
			deadPools[workerPoolID] = jobTypes
		}
	}

	return deadPools, nil
}

// checkPool reports whether a pool is dead, and the job types from its last heartbeat. When the
// heartbeat is gone, it returns the reaper's current job types.
func (r *deadPoolReaper) checkPool(conn redis.Conn, workerPoolID string) (bool, []string, error) {
	heartbeatKey := redisKeyHeartbeat(r.namespace, workerPoolID)
	heartbeatAt, err := redis.Int64(conn.Do("HGET", heartbeatKey, "heartbeat_at"))
	if err == redis.ErrNil {
		return true, r.curJobTypes, nil
	}
	if err != nil {
		return false, nil, err
	}

	// Check that last heartbeat was long enough ago to consider the pool dead
	if time.Unix(heartbeatAt, 0).Add(r.deadTime).After(time.Now()) {
		return false, nil, nil
	}

	jobTypesList, err := redis.String(conn.Do("HGET", heartbeatKey, "job_names"))
	if err == redis.ErrNil {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return true, strings.Split(jobTypesList, ","), nil
}

// findUnregisteredLockHolders returns pools that have lock info for a current job type but are not
// in the worker pools set. Such a pool left the set without releasing its locks, so a check of the
// set alone never finds it.
func (r *deadPoolReaper) findUnregisteredLockHolders(conn redis.Conn, workerPoolIDs []string) ([]string, error) {
	registered := make(map[string]struct{}, len(workerPoolIDs))
	for _, workerPoolID := range workerPoolIDs {
		registered[workerPoolID] = struct{}{}
	}

	var unregistered []string
	for _, jobType := range r.curJobTypes {
		poolIDs, err := redis.Strings(conn.Do("HKEYS", redisKeyJobsLockInfo(r.namespace, jobType)))
		if err != nil {
			return nil, err
		}
		for _, poolID := range poolIDs {
			if _, ok := registered[poolID]; !ok {
				registered[poolID] = struct{}{}
				unregistered = append(unregistered, poolID)
			}
		}
	}
	return unregistered, nil
}
