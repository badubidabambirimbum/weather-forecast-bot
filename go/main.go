// Программа для параллельного сбора данных
package main

import (
	"fmt"
	"log"
	"os/exec"
	"time"
)

// Структура для хранения путей к файлам exe
type File struct {
	Yandex   []string
	GisMeteo []string
}

// Функция для запуска exe файла
func parsing(file string, channel chan []byte) {
	cmd := exec.Command(file)

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
	file.Yandex = []string{"Moscow_Yandex/Moscow_Yandex.exe", "Krasnodar_Yandex/Krasnodar_Yandex.exe", "Ekaterinburg_Yandex/Ekaterinburg_Yandex.exe"}
	file.GisMeteo = []string{"Moscow_GisMeteo/Moscow_GisMeteo.exe", "Krasnodar_GisMeteo/Krasnodar_GisMeteo.exe", "Ekaterinburg_GisMeteo/Ekaterinburg_GisMeteo.exe"}
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
