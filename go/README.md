## :computer: Консольная программа на Go для сбора данных с сайтов

### Всего надо обновить 6 таблиц, поэтому создано 6 py файлов для параллельного запуска. 

```go
// Программа для параллельного сбора данных
package main

import (
	"fmt"
	"log"
	"os/exec"
	"time"
)

// Структура для хранения путей к файлам py
type File struct {
	Yandex   []string
	GisMeteo []string
}

// Функция для запуска py файла
func parsing(file string, channel chan []byte) {
	cmd := exec.Command("python", file)

	output, err := cmd.CombinedOutput()
	if err != nil {
		time.Sleep(5 * time.Minute) // На третий запрос в яндексе часто вылезает капча. Придется подождать
		output, err = cmd.CombinedOutput()
		if err != nil {
			log.Fatal(err)
		}
	}

	channel <- output
}

func main() {
	file := File{}
	file.Yandex = []string{"Moscow_Yandex.py", "Krasnodar_Yandex.py", "Ekaterinburg_Yandex.py"}
	file.GisMeteo = []string{"Moscow_GisMeteo.py", "Krasnodar_GisMeteo.py", "Ekaterinburg_GisMeteo.py"}
	n := len(file.GisMeteo)
	Yandex := make(chan []byte)
	GisMeteo := make(chan []byte)

	for _, file := range file.Yandex {
		go parsing(file, Yandex)
	}

	for _, file := range file.GisMeteo {
		go parsing(file, GisMeteo)
	}

	for i := 0; i < n; i++ {
		fmt.Print(<-GisMeteo)
		fmt.Print(<-Yandex)
	}
}

```