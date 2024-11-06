package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Job struct {
	ID             int       `json:"id"`
	Name           string    `json:"name"`
	ProcessingTime int       `json:"processing_time"` // Time in seconds
	SubmittedAt    time.Time `json:"submitted_at"`
	Status         string    `json:"status"`          // "queued" or "processing"
}

type JobQueue struct {
	jobs []Job
	mu   sync.Mutex
}

// Metrics for Prometheus
var (
	jobQueue = JobQueue{jobs: []Job{}}
	jobIDCounter = 1
	clients = make(map[*websocket.Conn]bool)
	upgrader = websocket.Upgrader{}

	totalJobSubmissions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "job_queue_submissions_total",
		Help: "Total number of job submissions",
	})

	queueLengthGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "job_queue_length",
		Help: "Current length of the job queue",
	})

	jobProcessingTimes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_processing_time_seconds",
		Help:    "Processing times for jobs in seconds",
		Buckets: prometheus.DefBuckets,
	})

	activeWebSocketConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_websocket_connections",
		Help: "Number of active WebSocket connections",
	})
)

func init() {
	// Register metrics with Prometheus
	prometheus.MustRegister(totalJobSubmissions)
	prometheus.MustRegister(queueLengthGauge)
	prometheus.MustRegister(jobProcessingTimes)
	prometheus.MustRegister(activeWebSocketConnections)
}

// Sort jobs by shortest processing time
func (jq *JobQueue) sortJobs() {
	sort.SliceStable(jq.jobs, func(i, j int) bool {
		return jq.jobs[i].ProcessingTime < jq.jobs[j].ProcessingTime
	})
}

// Broadcast job queue updates to all connected WebSocket clients
func broadcastQueueUpdate() {
	jobQueue.mu.Lock()
	defer jobQueue.mu.Unlock()

	data, _ := json.Marshal(jobQueue.jobs)
	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			client.Close()
			delete(clients, client)
			activeWebSocketConnections.Dec()
		}
	}
}

// Submit a new job to the queue
func submitJob(w http.ResponseWriter, r *http.Request) {
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Invalid job data", http.StatusBadRequest)
		return
	}

	job.ID = jobIDCounter
	jobIDCounter++
	job.SubmittedAt = time.Now()
	job.Status = "queued"

	jobQueue.mu.Lock()
	jobQueue.jobs = append(jobQueue.jobs, job)
	jobQueue.sortJobs()
	queueLengthGauge.Set(float64(len(jobQueue.jobs))) // Update queue length gauge
	jobQueue.mu.Unlock()

	// Increment job submission counter
	totalJobSubmissions.Inc()

	// Send update to WebSocket clients
	broadcastQueueUpdate()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// Get the current state of the job queue
func getQueueState(w http.ResponseWriter, r *http.Request) {
	jobQueue.mu.Lock()
	defer jobQueue.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobQueue.jobs)
}

// WebSocket handler for real-time updates
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}
	defer conn.Close()
	clients[conn] = true
	activeWebSocketConnections.Inc()

	// Initial broadcast of current queue state to new client
	jobQueue.mu.Lock()
	data, _ := json.Marshal(jobQueue.jobs)
	conn.WriteMessage(websocket.TextMessage, data)
	jobQueue.mu.Unlock()

	// Maintain the WebSocket connection
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			delete(clients, conn)
			activeWebSocketConnections.Dec()
			break
		}
	}
}

func main() {
	// Seed random for job processing time generation
	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/submit", submitJob)
	http.HandleFunc("/status", getQueueState)
	http.HandleFunc("/ws", handleWebSocket)

	// Expose Prometheus metrics at /metrics endpoint
	http.Handle("/metrics", promhttp.Handler())

	log.Println("Server started at :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
