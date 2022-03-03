package book

import (
	"github.com/pecid/rest-api-go-example/internal/domain"
)

type Service interface {
	GetAll() []domain.Book
	Save(title string, author string) (int, error)
}

type service struct {
	repository Repository
}

func NewService(r Repository) Service {
	return &service{
		repository: r,
	}
}

func (s *service) GetAll() []domain.Book {
	result := s.repository.GetAll()
	return result
}

func (s *service) Save(title string, author string) (int, error) {
	id, err := s.repository.Save(title, author)
	return id, err
}
