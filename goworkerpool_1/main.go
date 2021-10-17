package main

import (
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dbConnString   = "root@/db_test"
	dbMaxIdleConns = 4
	dbMaxConns     = 100
	totalWorker    = 100
)

type datatoto struct {
	Permainan string
	Nomor     string
	Bet       int
}
type datatotoResult struct {
	Permainan string
	Nomor     string
	Lastid    int64
	Status    string
}

var mutex sync.RWMutex

func main() {
	start := time.Now()
	db, err := openDBCon()
	if err != nil {
		log.Fatal(err.Error())
	}
	runtime.GOMAXPROCS(2)
	totals := 9999
	buffer := totals + 1
	jobs := make(chan datatoto, buffer)
	results := make(chan datatotoResult, buffer)

	totals_3d := 999
	buffer_3d := totals_3d + 1
	jobs_3d := make(chan datatoto, buffer_3d)
	results_3d := make(chan datatotoResult, buffer_3d)

	totals_2d := 99
	buffer_2d := totals_2d + 1
	jobs_2d := make(chan datatoto, buffer_2d)
	results_2d := make(chan datatotoResult, buffer_2d)

	wg := &sync.WaitGroup{}

	for w := 0; w < totalWorker; w++ {
		wg.Add(3)
		mutex.Lock()
		go doJob(w, jobs, results, db, wg)
		go doJob(w, jobs_3d, results_3d, db, wg)
		go doJob(w, jobs_2d, results_2d, db, wg)
		mutex.Unlock()
	}

	for i := 0; i <= totals; i++ {
		mutex.Lock()
		if i >= 0 && i < 10 {
			jobs <- datatoto{Permainan: "4D", Nomor: "000" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 10 && i <= 99 {
			jobs <- datatoto{Permainan: "4D", Nomor: "00" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 100 && i <= 999 {
			jobs <- datatoto{Permainan: "4D", Nomor: "0" + strconv.Itoa(i), Bet: 150}
		}
		if i > 999 && i <= 9999 {
			jobs <- datatoto{Permainan: "4D", Nomor: strconv.Itoa(i), Bet: 150}
		}
		mutex.Unlock()
	}
	for i := 0; i <= totals_3d; i++ {
		mutex.Lock()
		if i >= 0 && i < 10 {
			jobs_3d <- datatoto{Permainan: "3D", Nomor: "00" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 10 && i <= 99 {
			jobs_3d <- datatoto{Permainan: "3D", Nomor: "0" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 100 && i <= 999 {
			jobs_3d <- datatoto{Permainan: "3D", Nomor: strconv.Itoa(i), Bet: 150}
		}
		mutex.Unlock()
	}
	for i := 0; i <= totals_3d; i++ {
		mutex.Lock()
		if i >= 0 && i < 10 {
			jobs_3d <- datatoto{Permainan: "2D", Nomor: "0" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 10 && i <= 99 {
			jobs_3d <- datatoto{Permainan: "2D", Nomor: strconv.Itoa(i), Bet: 150}
		}
		mutex.Unlock()
	}
	close(jobs)
	close(jobs_3d)
	close(jobs_2d)
	for a := 0; a <= totals; a++ { //RESUL
		log.Println("Data Telah Disimpan :", <-results)
	}
	for a := 0; a <= totals_3d; a++ { //RESUL
		log.Println("Data Telah Disimpan 3D :", <-results_3d)
	}
	for a := 0; a <= totals_2d; a++ { //RESUL
		log.Println("Data Telah Disimpan 2D :", <-results_2d)
	}
	close(results)
	close(results_3d)
	close(results_3d)
	wg.Wait()

	elsapsed := time.Since(start)
	fmt.Println("Waktu :", elsapsed)
}
func openDBCon() (*sql.DB, error) {
	log.Println("=> Open Connection")

	db, err := sql.Open("mysql", dbConnString)

	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)
	return db, nil
}
func doJob(wId int, jobs <-chan datatoto, results chan<- datatotoResult, con *sql.DB, wg *sync.WaitGroup) {
	for capture := range jobs {
		for {
			var outerError error

			func(outerError *error) {
				defer func() {
					if err := recover(); err != nil {
						*outerError = fmt.Errorf("%v", err)
					}
				}()
				log.Printf("Terima data %s - %s - %d\n", capture.Permainan, capture.Nomor, capture.Bet)
				stmt, err := con.Prepare(`
						insert into
						tbl_test(
							permainan, nomor, bet 
						) values (
							?, ? , ?
						)
					`)
				if err != nil {
					log.Println(err.Error())
				}
				defer stmt.Close()
				res, err := stmt.Exec(capture.Permainan, capture.Nomor, capture.Bet)
				if err != nil {
					log.Println(err.Error())
				}
				LastInsertId, _ := res.LastInsertId()
				results <- datatotoResult{Permainan: capture.Permainan, Nomor: capture.Nomor, Lastid: LastInsertId, Status: "Done"}
			}(&outerError)
			if outerError == nil {
				break
			}
		}
	}
	wg.Done()
}
