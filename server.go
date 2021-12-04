package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

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
	Status    string     `json:"status"`
	JobID     string     `json:"job_id"`
	JobErrors []JobError `json:"error"`
}

type JobError struct {
	StoreID string `json:"store_id"`
	Error   string `json:"error"`
}
type JobOngoing struct {
	Status string `json:"status"`
	JobID  string `json:"job_id"`
}

func justDoIt(job Job, jobsCompleted *map[string]JobCompleted, jobsFailed *map[string]JobFailed, stores *map[string]Store) {
	var jobErrors []JobError
	for _, visit := range job.Payload.Visits {
		_, storePresent := (*stores)[visit.StoreID]
		var joberror JobError
		joberror.StoreID = visit.StoreID

		if !storePresent {
			job.Status = "failed"
			joberror.Error = "Store doesn't exists"
			jobErrors = append(jobErrors, joberror)
			continue
		}
		for _, imageURL := range visit.ImageURL {
			resp, err := http.Get(imageURL)
			if err != nil {
				job.Status = "failed"
				joberror.Error = "Unable to Download an Image"
				jobErrors = append(jobErrors, joberror)
				continue
			}
			defer resp.Body.Close()
			m, _, err := image.Decode(resp.Body)
			if err != nil {
				job.Status = "failed"
				joberror.Error = "Unable to Decode the Image"
				jobErrors = append(jobErrors, joberror)
				continue
			}

			// calculating perimeter to show some processing and random sleep time as said in assignment
			g := m.Bounds()
			height := g.Dy()
			width := g.Dx()
			perimeter := 2 * (height + width)
			perimeter += 0 // does nothing, just to remove the not using variable error
			// fmt.Println(perimeter)
			pointSeconds := rand.Intn(3) + 1
			time.Sleep((time.Second / 10) * time.Duration(pointSeconds))
		}
	}

	jobid := job.JobID
	if job.Status != "failed" {
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
		jobFailed.JobErrors = jobErrors
		(*jobsFailed)[jobid] = jobFailed
	}
}
func jobHandlerWrapper(stores *map[string]Store, jobs *map[string]Job, jobsCompleted *map[string]JobCompleted, jobsFailed *map[string]JobFailed, nextJobID *int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Payload Validation Initiated
		var payload Payload
		_ = json.NewDecoder(r.Body).Decode(&payload)

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

		for _, visit := range payload.Visits {

			if len(visit.ImageURL) == 0 || visit.StoreID == "" || visit.VisitTime == "" {
				error.Error = "Field missing or empty in request payload"
				json.NewEncoder(w).Encode(error)
				return
			}

		}
		// Payload Validation Complete

		// Now assign a job and start a goroutine to process the job
		var job Job
		job.JobID = strconv.Itoa(*nextJobID)
		job.Status = "ongoing"
		job.Payload = payload
		(*jobs)[job.JobID] = job

		go justDoIt(job, jobsCompleted, jobsFailed, stores)

		// send back success of job registration

		var success Success
		success.JobID = strconv.Itoa(*nextJobID)
		json.NewEncoder(w).Encode(success)
		(*nextJobID)++
	}
}
func jobInfoHandlerWrapper(jobs *map[string]Job, jobsCompleted *map[string]JobCompleted, jobsFailed *map[string]JobFailed) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queryJobID := r.URL.Query().Get("jobid")

		jobid := queryJobID
		_, jobIDExists := (*jobs)[jobid]
		if !jobIDExists {
			fmt.Println("Error", queryJobID)
			json.NewEncoder(w).Encode("{}")
			return
		} else {
			_, present := (*jobsCompleted)[jobid]
			if present {
				json.NewEncoder(w).Encode((*jobsCompleted)[jobid])
				return
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

func main() {
	router := mux.NewRouter()
	var stores = make(map[string]Store)
	var jobs = make(map[string]Job)
	var jobsCompleted = make(map[string]JobCompleted)
	var jobsFailed = make(map[string]JobFailed)
	nextJobID := 1
	// loading store data
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
			stores[store.StoreID] = store
		}
	}
	// store data loaded

	// Route Handlers
	router.HandleFunc("/api/submit", jobHandlerWrapper(&stores, &jobs, &jobsCompleted, &jobsFailed, &nextJobID)).Methods("POST")
	router.HandleFunc("/api/status", jobInfoHandlerWrapper(&jobs, &jobsCompleted, &jobsFailed)).Methods("GET")

	log.Fatal(http.ListenAndServe(":8000", router))

}
