// Программа для параллельного сбора данных
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Структура для хранения путей к файлам py
type File struct {
	Yandex   []string
	GisMeteo []string
	Backup   []string
}

// Функция для чтения путей из файла и распределения их по структуре File
func readFiles(filename string) (File, error) {
	var file File
	var currentCategory *([]string)

	files, err := os.Open(filename)
	if err != nil {
		return file, err
	}
	defer files.Close()

	scanner := bufio.NewScanner(files)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") {
			// Определяем текущую категорию
			if strings.Contains(line, "Yandex") {
				currentCategory = &file.Yandex
			} else if strings.Contains(line, "GisMeteo") {
				currentCategory = &file.GisMeteo
			} else if strings.Contains(line, "Backup") {
				currentCategory = &file.Backup
			}
		} else if currentCategory != nil && line != "" {
			*currentCategory = append(*currentCategory, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return file, err
	}

	return file, nil
}

// Функция для запуска py файла
func executionFiles(file string, wg *sync.WaitGroup, channel chan []byte) {
	defer wg.Done()

	cmd := exec.Command("python", file)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Ошибка выполнения файла %s: %v", file, err)
		time.Sleep(5 * time.Minute) // На третий запрос в яндексе часто вылезает капча. Придется подождать
		output, err = cmd.CombinedOutput()
		if err != nil {
			log.Fatalf("Не удалось выполнить файл %s после повторной попытки: %v", file, err)
			return
		}
	}

	channel <- output
}

func main() {
	// Чтение путей из файла
	file, err := readFiles("files.txt")
	if err != nil {
		log.Fatalf("Ошибка при чтении файла путей: %v", err)
	}
	var wg sync.WaitGroup

	Backup := make(chan []byte)
	Yandex := make(chan []byte)
	GisMeteo := make(chan []byte)

	// Делаем backup перед обновлением баз данных
	wg.Add(1)
	go executionFiles(file.Backup[0], &wg, Backup)
	result := <-Backup
	fmt.Printf("[%d/%d] %30s\n", 1, 1, string(result))
	close(Backup)

	for _, file := range file.Yandex {
		wg.Add(1)
		go executionFiles(file, &wg, Yandex)
	}

	for _, file := range file.GisMeteo {
		wg.Add(1)
		go executionFiles(file, &wg, GisMeteo)
	}

	// Горутина для закрытия каналов после завершения всех горутин
	go func() {
		wg.Wait()
		close(Yandex)
		close(GisMeteo)
	}()

	idx := 1

	// Обработка результатов из обоих каналов
	for {
		select {
		case result, ok := <-Yandex:
			if !ok {
				Yandex = nil
			} else {
				fmt.Printf("[%d/%d] %30s", idx, len(file.Yandex)+len(file.GisMeteo), string(result))
				idx++
			}
		case result, ok := <-GisMeteo:
			if !ok {
				GisMeteo = nil
			} else {
				fmt.Printf("[%d/%d] %30s", idx, len(file.Yandex)+len(file.GisMeteo), string(result))
				idx++
			}
		case <-time.After(5 * time.Second):
			if Yandex == nil && GisMeteo == nil {
				return
			}
		}
	}
}
