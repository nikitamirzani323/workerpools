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
	totalWorker    = 200
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
	totals_5d := 99999
	buffer_5d := totals_5d + 1
	jobs_5d := make(chan datatoto, buffer_5d)
	results_5d := make(chan datatotoResult, buffer_5d)

	totals_4d := 9999
	buffer_4d := totals_4d + 1
	jobs_4d := make(chan datatoto, buffer_4d)
	results_4d := make(chan datatotoResult, buffer_4d)

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
		wg.Add(4)
		mutex.Lock()
		go doJob(w, jobs_5d, results_5d, db, wg)
		go doJob(w, jobs_4d, results_4d, db, wg)
		go doJob(w, jobs_3d, results_3d, db, wg)
		go doJob(w, jobs_2d, results_2d, db, wg)
		mutex.Unlock()
	}
	for i := 0; i <= totals_5d; i++ {
		mutex.Lock()
		if i >= 0 && i < 10 {
			jobs_5d <- datatoto{Permainan: "5D", Nomor: "0000" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 10 && i <= 99 {
			jobs_5d <- datatoto{Permainan: "5D", Nomor: "000" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 100 && i <= 999 {
			jobs_5d <- datatoto{Permainan: "5D", Nomor: "00" + strconv.Itoa(i), Bet: 150}
		}
		if i > 999 && i <= 9999 {
			jobs_5d <- datatoto{Permainan: "5D", Nomor: "0" + strconv.Itoa(i), Bet: 150}
		}
		if i > 9999 && i <= 99999 {
			jobs_5d <- datatoto{Permainan: "5D", Nomor: strconv.Itoa(i), Bet: 150}
		}
		mutex.Unlock()
	}
	for i := 0; i <= totals_4d; i++ {
		mutex.Lock()
		if i >= 0 && i < 10 {
			jobs_4d <- datatoto{Permainan: "4D", Nomor: "000" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 10 && i <= 99 {
			jobs_4d <- datatoto{Permainan: "4D", Nomor: "00" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 100 && i <= 999 {
			jobs_4d <- datatoto{Permainan: "4D", Nomor: "0" + strconv.Itoa(i), Bet: 150}
		}
		if i > 999 && i <= 9999 {
			jobs_4d <- datatoto{Permainan: "4D", Nomor: strconv.Itoa(i), Bet: 150}
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
	for i := 0; i <= totals_2d; i++ {
		mutex.Lock()
		if i >= 0 && i < 10 {
			jobs_2d <- datatoto{Permainan: "2D", Nomor: "0" + strconv.Itoa(i), Bet: 150}
		}
		if i >= 10 && i <= 99 {
			jobs_2d <- datatoto{Permainan: "2D", Nomor: strconv.Itoa(i), Bet: 150}
		}
		mutex.Unlock()
	}
	close(jobs_5d)
	close(jobs_4d)
	close(jobs_3d)
	close(jobs_2d)

	for a := 0; a <= totals_5d; a++ { //RESUL
		log.Println("Data Telah Disimpan 5D :", <-results_5d)
	}
	for a := 0; a <= totals_4d; a++ { //RESUL
		log.Println("Data Telah Disimpan 4D :", <-results_4d)
	}
	for a := 0; a <= totals_3d; a++ { //RESUL
		log.Println("Data Telah Disimpan 3D :", <-results_3d)
	}
	for a := 0; a <= totals_2d; a++ { //RESUL
		log.Println("Data Telah Disimpan 2D :", <-results_2d)
	}
	close(results_5d)
	close(results_4d)
	close(results_3d)
	close(results_2d)
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
