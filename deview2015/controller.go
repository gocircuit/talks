package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gocircuit/circuit/client"
)

type Controller struct {
	client       *client.Client      // Connection to a circuit server.
	maxPerWorker int                 // Maximum number of concurrent jobs per worker.
	sync.Mutex                       // Protects the fields below.
	job          map[string]struct{} // Use to enforce unique job names.
	worker       map[string]*worker  // Set of known workers and jobs running on them.
	pending      []*job              // Set of pending jobs.
}

type job struct {
	name string
	cmd  client.Cmd
}

type worker struct {
	name    string
	running []*job
}

// removeJobFromWorkers removes the given job from the worker's list of running jobs.
func (w *worker) removeJobFromWorkers(j *job) {
	for i, job := range w.running {
		if job == j {
			m := len(w.running) - 1
			w.running[i] = w.running[m]
			w.running = w.running[:m]
			return
		}
	}
	panic("job not found on worker")
}

func NewController(client *client.Client, maxJobsPerWorker int) *Controller {
	c := &Controller{
		client:       client,
		job:          make(map[string]struct{}),
		maxPerWorker: maxJobsPerWorker,
		worker:       make(map[string]*worker),
	}
	c.subscribeToJoin()
	c.subscribeToLeave()
	return c
}

// subscribeToJoin creates a new subscription for notifications about joining workers (i.e. hosts).
func (s *Controller) subscribeToJoin() {
	joinAnchor := s.client.Walk([]string{s.client.ServerID(), "controller", "join"})
	joinAnchor.Scrub()
	onJoin, err := joinAnchor.MakeOnJoin()
	if err != nil {
		log.Fatalf("Another controller running on this circuit server.")
	}
	go func() {
		for {
			x, ok := onJoin.Consume()
			if !ok {
				log.Fatal("Circuit disappeared.")
			}
			s.workerStarted(x.(string))
		}
	}()
}

// subscribeToLeave creates a new subscription for notifications about leaving (i.e. dying) workers.
func (s *Controller) subscribeToLeave() {
	leaveAnchor := s.client.Walk([]string{s.client.ServerID(), "controller", "leave"})
	leaveAnchor.Scrub()
	onLeave, err := leaveAnchor.MakeOnLeave()
	if err != nil {
		log.Fatalf("Another controller running on this circuit server.")
	}
	go func() {
		for {
			x, ok := onLeave.Consume()
			if !ok {
				log.Fatal("Circuit disappeared.")
			}
			s.workerDied(x.(string))
		}
	}()
}

func parseWorkerName(s string) (string, bool) {
	name := strings.TrimSpace(s)
	if len(name) < 2 || name[0] != '/' {
		log.Printf("Unrecognized worker: %q", s)
		return "", false
	}
	return name[1:], true
}

func (s *Controller) workerStarted(name string) {
	s.Lock()
	defer s.Unlock()
	var parsed bool
	if name, parsed = parseWorkerName(name); !parsed {
		return
	}
	if _, ok := s.worker[name]; ok {
		return
	}
	log.Printf("Worker %s started.", name)
	s.worker[name] = &worker{name: name}
	go s.schedule()
}

func (s *Controller) workerDied(name string) {
	s.Lock()
	defer s.Unlock()
	var parsed bool
	if name, parsed = parseWorkerName(name); !parsed {
		return
	}
	if _, ok := s.worker[name]; !ok {
		return
	}
	log.Printf("Worker %s died.", name)
	delete(s.worker, name)
	// Jobs on this worker will be rescheduled by the startJob job controllers.
}

// AddJob adds a new job for scheduling.
// The name parameter must identify the job uniquely.
// The cmd parameter holds the execution specification for the job.
// An error is returned if a job with the same name is already in the scheduler.
func (s *Controller) AddJob(name string, cmd client.Cmd) error {
	s.Lock()
	defer s.Unlock()
	if _, present := s.job[name]; present {
		return fmt.Errorf("Duplicate job name.")
	}
	s.job[name] = struct{}{}
	log.Printf("Added job %s.", name)
	cmd.Scrub = true
	s.pending = append(s.pending, &job{name, cmd})
	go s.schedule()
	return nil
}

func (s *Controller) jobDone(w *worker, j *job, exit error) {
	s.Lock()
	defer s.Unlock()
	if exit == nil {
		log.Printf("Job %s completed on %s.", j.name, w.name)
	} else {
		log.Printf("Job %s failed on %s: %v", j.name, w.name, exit)
	}
	w.removeJobFromWorkers(j)
	delete(s.job, j.name)
	go s.schedule()
}

func (s *Controller) rescheduleJob(w *worker, j *job) {
	s.Lock()
	defer s.Unlock()
	log.Printf("Rescheduling job %s.", j.name)
	time.Sleep(time.Second)
	w.removeJobFromWorkers(j)
	s.pending = append([]*job{j}, s.pending...)
	go s.schedule()
}

type slot struct {
	worker *worker
	token  int // Number of jobs that can be added to the worker.
}

// schedule finds workers that can accommodate more jobs and runs any
// pending jobs on them, updating the state of the scheduler respectively.
func (s *Controller) schedule() {
	s.Lock()
	defer s.Unlock()
	// Prepare list of workers with capacity to run more jobs.
	var slots []*slot
	for _, w := range s.worker {
		if len(w.running) >= s.maxPerWorker {
			continue
		}
		slots = append(slots, &slot{w, s.maxPerWorker - len(w.running)})
	}
	// Start jobs on the available workers.
	for len(s.pending) > 0 && len(slots) > 0 {
		// Pop job from queue.
		job := s.pending[0]
		s.pending = s.pending[1:]
		// Pop slot.
		worker := slots[0].worker
		slots[0].token--
		if slots[0].token == 0 {
			slots = slots[1:]
		}
		// Start job.
		worker.running = append(worker.running, job) // Assign job to worker.
		go s.startJob(worker, job)
	}
}

func (s *Controller) startJob(worker *worker, job *job) {
	defer func() {
		// Physical errors (like network malfunction or worker death) are delivered as panics
		// by the methods of the Circuit client package.
		if r := recover(); r != nil { // Worker died before job completed.
			s.rescheduleJob(worker, job)
		}
	}()
	// Obtain anchor for the job.
	jobAnchor := s.client.Walk([]string{worker.name, "job", job.name})
	// Remove any leftover processes running on this anchor, possibly from a prior scheduler.
	jobAnchor.Scrub()
	if prior := jobAnchor.Get(); prior != nil {
		if p, ok := prior.(client.Proc); ok {
			p.Signal("KILL")
		}
	}
	jobAnchor.Scrub()
	// Run the job.
	p, err := jobAnchor.MakeProc(job.cmd)
	if err != nil {
		// An error condition can occur here, only if the anchor already holds an object.
		// Since we made sure the anchor is free (above), this condition indicates
		// that a competing application is using the same anchor.
		log.Printf("It appears a competing scheduler is running. Collision on anchor %s.", jobAnchor.Path())
		s.jobDone(worker, job, err)
		return
	}
	// POSIX requires that the standard input of a child process is explicitly closed.
	p.Stdin().Close()
	// POSIX requires that the standard output and error of a child process are consumed,
	// otherwise the child process would block. The following goroutines ensure that.
	go func() {
		defer func() {
			recover() // Ignore physical errors (network connectivity or host failure).
		}()
		// Drain and discard the standard output of the child process.
		io.Copy(drain{}, p.Stdout())
	}()
	go func() {
		defer func() {
			recover() // Ignore physical errors (network connectivity or host failure).
		}()
		// Drain and discard the standard error of the child process.
		io.Copy(drain{}, p.Stderr())
	}()
	_, err = p.Wait() // Block until the process completes.
	// Wait returns a non-nil error only if the process was explicitly aborted/scrubbed by the user.
	s.jobDone(worker, job, err)
}

// drain is an io.Writer that discards everything written to it.
type drain struct{}

func (drain) Write(p []byte) (int, error) {
	return len(p), nil
}

// String returns a textual representation of the state of the scheduler.
func (s *Controller) String() string {
	s.Lock()
	defer s.Unlock()
	var w bytes.Buffer
	fmt.Fprintln(&w, "running: ")
	for _, wrk := range s.worker {
		fmt.Fprintf(&w, "%s: ", wrk.name)
		for _, j := range wrk.running {
			fmt.Fprintf(&w, "%s, ", j.name)
		}
		fmt.Fprintln(&w)
	}
	fmt.Fprintln(&w, "\npending: ")
	for _, j := range s.pending {
		fmt.Fprintf(&w, "%s, ", j.name)
	}
	fmt.Fprintln(&w)
	return w.String()
}
