package book

import (
	"github.com/jinzhu/gorm"
	"github.com/pecid/rest-api-go-example/internal/domain"
)

type Repository interface {
	GetAll() []domain.Book
	Get(id int) (domain.Book, error)
	Exists(id int) bool
	Save(title string, author string) (int, error) //return id or error
	Update(id int, new domain.Book) error          // true or false or error
	Delete(id int) error
}

type repository struct {
	DB *gorm.DB
}

func NewRespository(db *gorm.DB) Repository {
	return &repository{
		DB: db,
	}
}

func (r *repository) GetAll() []domain.Book {
	var books []domain.Book // array
	r.DB.Find(&books)
	return books
}

func (r *repository) Get(id int) (domain.Book, error) {
	var book domain.Book
	err := r.DB.Where("id = ?", id).First(&book).Error
	if err != nil {
		return book, err
	}
	return book, nil
}

func (r *repository) Delete(id int) error {
	var book domain.Book
	error := r.DB.Where("id = ?", id).First(&book).Error
	if error != nil {
		return error
	}
	r.DB.Delete(&book)
	return error
}

func (r *repository) Exists(id int) bool {
	state := true
	error := r.DB.Where("id = ?", id).Error
	if error != nil {
		state = false
	}
	return state
}

func (r *repository) Save(title string, author string) (int, error) {
	book := domain.Book{
		Title:  title,
		Author: author,
	}
	r.DB.Create(&book)
	return int(book.ID), nil
}

func (r *repository) Update(id int, new domain.Book) error {
	var book domain.Book
	error := r.DB.Where("id = ?", id).First(&book).Error
	if error != nil {
		return error
	}

	r.DB.Model(&book).Updates(new)
	return nil
}
