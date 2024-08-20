package main

import (
	"fmt"
	"log"
	"os/exec"
	"time"
)

func parsing(file string, channel chan string) {
	cmd := exec.Command(file)

	output, err := cmd.CombinedOutput()
	if err != nil {
		time.Sleep(2 * time.Minute)
		output, err = cmd.CombinedOutput()
		if err != nil {
			log.Fatal(err)
		}
	}

	channel <- string(output)
}

func main() {
	Yandex := make(chan string)
	GisMeteo := make(chan string)

	go parsing("Moscow_GisMeteo/Moscow_GisMeteo.exe", GisMeteo)
	go parsing("Moscow_Yandex/Moscow_Yandex.exe", Yandex)

	go parsing("Krasnodar_GisMeteo/Krasnodar_GisMeteo.exe", GisMeteo)
	go parsing("Krasnodar_Yandex/Krasnodar_Yandex.exe", Yandex)

	go parsing("Ekaterinburg_GisMeteo/Ekaterinburg_GisMeteo.exe", GisMeteo)
	go parsing("Ekaterinburg_Yandex/Ekaterinburg_Yandex.exe", Yandex)

	fmt.Print(<-Yandex)
	fmt.Print(<-GisMeteo)

	fmt.Print(<-Yandex)
	fmt.Print(<-GisMeteo)

	fmt.Print(<-Yandex)
	fmt.Print(<-GisMeteo)
}
