package bigquery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pecid/rest-api-go-example/internal/domain"
	"google.golang.org/api/iterator"
)

type Block struct {
	Try     func()
	Catch   func(Exception)
	Finally func()
}

type Exception interface{}

func Throw(up Exception) {
	panic(up)
}

func (tcf Block) Do() {
	if tcf.Finally != nil {

		defer tcf.Finally()
	}
	if tcf.Catch != nil {
		defer func() {
			if r := recover(); r != nil {
				tcf.Catch(r)
			}
		}()
	}
	tcf.Try()
}

// ----------------------------------------------------------------------------
type BigQueryService interface {
	Read(ctx context.Context) (*Response, error)
}

type bigQuery struct{}

type Response struct {
	Data     []domain.Person `json:"data"`
	DataTime []time.Duration `json:"data_time"`
	PromTime string          `json:"prom_time"`
}

func NewBigQueryService() BigQueryService {
	return &bigQuery{}
}

// 7 min to process 1 Million rows
func (b *bigQuery) Read(ctx context.Context) (*Response, error) {
	projectID := "applied-abbey-341819"
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()
	init := time.Now()
	q := client.Query("SELECT * FROM `applied-abbey-341819.datos_prueba.datos` LIMIT 1000")
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = "southamerica-west1"
	// Run the query and print results when the query job is completed.
	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	it, err := job.Read(ctx)
	endInit := time.Since(init)
	fmt.Println("Init: ", endInit)
	if err != nil {
		return nil, fmt.Errorf("job.Read(): %v", err)
	}
	var data []domain.Person
	var timeData []time.Duration
	cont := 1
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	timeSum := time.Duration(0)
	for {
		start := time.Now()
		var row []bigquery.Value
		err := it.Next(&row)
		endStart := time.Since(start)

		timeData = append(timeData, endStart)
		if err != nil && err != iterator.Done {
			return nil, err
		}
		if err == iterator.Done {
			fmt.Println("End")
			break
		} else {
			startGoRoutine := time.Now()
			wg.Add(10)
			go func() {
				mutex.Lock()
				fmt.Println("Start ", cont, " ", endStart.String())
				Block{
					Try: func() {
						data = append(data, domain.Person{
							Nombre:   row[0].(string),
							Apellido: row[1].(string),
							Ciudad:   row[2].(string),
						})
					},
					Catch: func(e Exception) {
						fmt.Printf("Caught %v\n", e)
					},
					Finally: func() {},
				}.Do()
				cont++
				wg.Add(-10)
				mutex.Unlock()
			}()
			endGoRoutine := time.Since(startGoRoutine)
			timeSum += endGoRoutine
		}
	}
	wg.Wait()
	timeTotal := timeSum / time.Duration(len(timeData))
	response := &Response{
		Data:     data[:3],
		DataTime: timeData[:3],
		PromTime: timeTotal.String(),
	}
	return response, nil
}
