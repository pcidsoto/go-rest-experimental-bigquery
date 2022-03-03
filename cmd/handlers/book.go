package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pecid/rest-api-go-example/internal/book"
)

type Product struct {
	service book.Service
}

func NewProduct(s book.Service) *Product {
	return &Product{
		service: s,
	}
}

func (p *Product) GetAll() gin.HandlerFunc {
	return func(context *gin.Context) {
		response := p.service.GetAll()
		context.JSON(http.StatusOK, gin.H{"data": response})
	}
}

func (p *Product) Store() gin.HandlerFunc {
	type Request struct {
		ID     int    `json:"id"`
		Title  string `json:"title"`
		Author string `json:"author"`
	}

	return func(context *gin.Context) {
		var request Request
		if err := context.Bind(&request); err != nil {
			context.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		id, err := p.service.Save(request.Title, request.Author)
		if err != nil {
			context.JSON(http.StatusBadRequest, gin.H{})
			return
		}
		context.JSON(http.StatusCreated, gin.H{
			"id": id,
		})

	}
}
