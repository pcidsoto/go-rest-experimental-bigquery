package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pecid/rest-api-go-example/internal/bigquery"
)

type BigQuery struct {
	service bigquery.BigQueryService
}

func NewBigQuery(s bigquery.BigQueryService) *BigQuery {
	return &BigQuery{
		service: s,
	}
}

func (p *BigQuery) ReadAll() gin.HandlerFunc {
	return func(context *gin.Context) {
		response, err := p.service.Read(context)
		if err != nil {
			context.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}
		context.JSON(http.StatusOK, gin.H{"data": response})
	}
}
