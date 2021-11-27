package main

import (
	"encoding/csv"
	"encoding/json"
	"image"
	"log"
	"math/rand"
	"strconv"
	"time"

	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

type Visit struct {
	StoreID   string   `json:"store_id"`
	ImageURL  []string `json:"image_url"`
	VisitTime string   `json:"visit_time"`
}

type Payload struct {
	Count  int     `json:"count"`
	Visits []Visit `json:"visits"`
}

type Success struct {
	JobID string `json:"job_id"`
}

type Error struct {
	Error string `json:"error"`
}

type Store struct {
	AreaCode  string `json:"area_code"`
	StoreName string `json:"store_name`
	StoreID   string `json:"store_id"`
}

type Job struct {
	JobID   string  `json:"job_id"`
	Payload Payload `json:"payload"`
	Status  string  `json:"status"`
}

type JobCompleted struct {
	Status string `json:"status"`
	JobID  string `json:"job_id"`
}
type JobFailed struct {
	Status   string     `json:"status"`
	JobID    string     `json:"job_id"`
	JobError []JobError `json:"error"`
}

type JobError struct {
	StoreID string `json:"store_id"`
	Error   string `json:"error"`
}
type JobOngoing struct {
	Status string `json:"status"`
	JobID  string `json:"job_id"`
}

var nextJobID int = 1

func jobHandlerWrapper(stores []Store, jobs *map[string]Job) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var payload Payload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		// fmt.Println(x)
		var error Error
		if len(payload.Visits) != payload.Count {
			error.Error = "count is not equal to the length of the array property visits"
			json.NewEncoder(w).Encode(error)
			return
		} else if payload.Count == 0 {
			error.Error = "Invalid Payload"
			json.NewEncoder(w).Encode(error)
			return
		}

		// fmt.Printf("%+v\n", payload)

		for _, visit := range payload.Visits {

			if len(visit.ImageURL) == 0 || visit.StoreID == "" || visit.VisitTime == "" {
				error.Error = "Field missing or empty in request payload"
				json.NewEncoder(w).Encode(error)
				return
			}

		}

		var job Job
		job.JobID = strconv.Itoa(nextJobID)
		job.Status = "ongoing"
		job.Payload = payload
		(*jobs)[job.JobID] = job

		var success Success
		success.JobID = strconv.Itoa(nextJobID)
		json.NewEncoder(w).Encode(success)
		nextJobID++
		fmt.Println("confirmed")
	}
}
func jobInfoHandlerWrapper(jobs *map[string]Job, jobsCompleted *map[string]JobCompleted, jobsFailed *map[string]JobFailed) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queryJobID := r.URL.Query().Get("jobid")
		jobIDExists := false
		fmt.Println(jobs)
		for i, job := range *jobs {
			fmt.Println(i)
			fmt.Println(job.JobID)
			if job.JobID == queryJobID {
				jobIDExists = true
				break
			}
		}
		var jobid string
		jobid = queryJobID
		if !jobIDExists {
			fmt.Println("Error", queryJobID)
			json.NewEncoder(w).Encode("{}")
			return
		} else {
			_, present := (*jobsCompleted)[jobid]
			if present {
				json.NewEncoder(w).Encode((*jobsCompleted)[jobid])
			}
			_, failed := (*jobsFailed)[jobid]
			if failed {
				json.NewEncoder(w).Encode((*jobsFailed)[jobid])
			} else {
				var ongoingMessage JobOngoing
				ongoingMessage.Status = "ongoing"
				ongoingMessage.JobID = jobid

				json.NewEncoder(w).Encode(ongoingMessage)
			}

		}
	}
}
func backgroundWorker(jobs *map[string]Job, jobsCompleted *map[string]JobCompleted, jobsFailed *map[string]JobFailed, stores []Store) {
	for _, job := range *jobs {
		var failedJob JobFailed
		failedJob.Status = "failed"
		failedJob.Status = job.JobID
		for _, visit := range job.Payload.Visits {

			for _, imageURL := range visit.ImageURL {

				storePresent := false
				for _, store := range stores {
					if store.StoreID == visit.StoreID {
						storePresent = true
					}
				}

				if !storePresent {
					job.Status = "failed"
					var joberror JobError
					joberror.StoreID = visit.StoreID
					joberror.Error = "Store doesn't exists"
					continue
				}

				resp, err := http.Get(imageURL)
				if err != nil {
					job.Status = "failed"
					var joberror JobError
					joberror.StoreID = visit.StoreID
					joberror.Error = "Unable to Download an Image"
					continue
				}
				defer resp.Body.Close()
				m, _, err := image.Decode(resp.Body)
				if err != nil {
					job.Status = "failed"
					continue
				}
				g := m.Bounds()
				height := g.Dy()
				width := g.Dx()

				perimeter := 2 * (height + width)
				if perimeter < 0 {
					fmt.Println("Impossible!!")
				}
				pointSeconds := rand.Intn(3) + 1
				time.Sleep((time.Second / 10) * time.Duration(pointSeconds))
			}
		}
		var jobid string
		jobid = job.JobID

		if job.Status == "ongoing" {
			job.Status = "completed"
			var jobCompleted JobCompleted
			jobCompleted.Status = "completed"
			jobCompleted.JobID = job.JobID
			(*jobsCompleted)[jobid] = jobCompleted
		} else {
			job.Status = "failed"
			var jobFailed JobFailed
			jobFailed.Status = "failed"
			jobFailed.JobID = job.JobID
			(*jobsFailed)[jobid] = jobFailed
		}
	}

}
func main() {
	router := mux.NewRouter()
	var stores []Store
	var jobs = make(map[string]Job)
	var jobsCompleted = make(map[string]JobCompleted)
	var jobsFailed = make(map[string]JobFailed)

	csvFile, err := os.Open("StoreMasterAssignment.csv")

	if err != nil {
		fmt.Println(err)
	}
	defer csvFile.Close()

	csvLines, err := csv.NewReader(csvFile).ReadAll()

	if err != nil {
		fmt.Println(err)
	}
	for i, line := range csvLines {
		store := Store{
			AreaCode:  line[0],
			StoreName: line[1],
			StoreID:   line[2],
		}
		if i > 0 {
			stores = append(stores, store)
		}
	}

	// Route Handlers
	router.HandleFunc("/api/submit", jobHandlerWrapper(stores, &jobs)).Methods("POST")
	router.HandleFunc("/api/status", jobInfoHandlerWrapper(&jobs, &jobsCompleted, &jobsFailed)).Methods("GET")
	go backgroundWorker(&jobs, &jobsCompleted, &jobsFailed, stores)

	log.Fatal(http.ListenAndServe(":8000", router))

}
