package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pecid/rest-api-go-example/cmd/handlers"
	"github.com/pecid/rest-api-go-example/internal/bigquery"
	"github.com/pecid/rest-api-go-example/internal/book"
	"github.com/pecid/rest-api-go-example/models"
)

var DB *gorm.DB

func main() {
	server := gin.Default()

	database, error := gorm.Open("sqlite3", "test.db")

	if error != nil {
		fmt.Println("error: ", error)
		panic("Failed to connect to database")
	}

	database.AutoMigrate(&models.Book{})

	DB = database

	bookRepo := book.NewRespository(DB)
	bookServ := book.NewService(bookRepo)
	bookHandler := handlers.NewProduct(bookServ)

	bigQueryServ := bigquery.NewBigQueryService()
	bigQueryHandler := handlers.NewBigQuery(bigQueryServ)

	server.GET("/books", bookHandler.GetAll())
	server.POST("/books", bookHandler.Store())
	server.GET("/bigquery", bigQueryHandler.ReadAll())

	server.Run(":8082")
}

//$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\Pedro\Documents\golang experiments\rest-api-go-example\claves\applied-abbey-341819-e4828eb07cbe.json"
