package grades

import (
	"fmt"
	"sync"
)

type Student struct {
	ID        uint
	FirstName string
	LastName  string
	Grades    []Grade
}

func (s Student) Average() float32 {
	var result float32
	for _, grade := range s.Grades {
		result += grade.Score
	}
	return result / float32(len(s.Grades))
}

type Students []Student

func (s Students) GetByID(id uint) (*Student, error) {
	for i := range s {
		if s[i].ID == id {
			return &s[i], nil
		}
	}
	return nil, fmt.Errorf("Student with ID %v does not exist", id)
}

var (
	students      Students
	studentsMutex sync.Mutex
)

type GradeType string

const (
	GradeTest     = GradeType("Test")
	GradeHomework = GradeType("Homework")
	GradeQuiz     = GradeType("Quiz")
)

type Grade struct {
	Title string
	Type  GradeType
	Score float32
}
