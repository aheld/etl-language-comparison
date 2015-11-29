package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
)

var wg sync.WaitGroup
var wg2 sync.WaitGroup

func main() {
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	var input = "../tmp/tweets/"

	filenames := make(chan string)
	counts := make(chan Stat)

	wg.Add(1)
	go reduce(counts)
	for i := 0; i < cpus*2; i++ {
		wg2.Add(1)
		go func() {
			parse_file(filenames, counts)
			wg2.Done()
		}()
	}

	filelist, _ := ioutil.ReadDir(input)
	for _, file := range filelist {
		filenames <- filepath.Join(input, file.Name())
	}
	close(filenames)

	go func() {
		wg2.Wait()
		close(counts)
	}()

	wg.Wait()
}

func parse_file(filenames chan string, counts chan Stat) {
	reKnicks := regexp.MustCompile("(?i)knicks")
	for file := range filenames {
		tweet_file, _ := os.OpenFile(file, os.O_RDONLY, 0666)
		defer tweet_file.Close()
		reader := bufio.NewScanner(tweet_file)
		for reader.Scan() {
			l := reader.Text()
			record := strings.Split(l, "\t")
			hood := record[1]
			tweet := record[3]
			wg2.Add(1)
			go func(tweet string, hood string) {
				if reKnicks.MatchString(tweet) {
					counts <- Stat{Hood: hood, Count: 1}
				}
				wg2.Done()
			}(tweet, hood)
		}
	}
}

func reduce(counts chan Stat) {
	result := make(map[string]int)
	for count := range counts {
		result[count.Hood]++
	}
	stats := make([]Stat, 0, len(result))
	for k, v := range result {
		stats = append(stats, Stat{Hood: k, Count: v})
	}
	sort.Sort(Stats(stats))

	generateOutput(stats, "golang_out_channel")
	wg.Done()

}

func generateOutput(stats []Stat, output string) {
	os.MkdirAll(filepath.Dir(output), 0755)

	file, err := os.Create(output)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for _, hoodcount := range stats {
		rval := fmt.Sprintf("%s\t%d\n", hoodcount.Hood, hoodcount.Count)
		file.WriteString(rval)
	}
}

type Stat struct {
	Hood  string
	Count int
}

// comparisons to make Stat support the Sort interface
type Stats []Stat

func (a Stats) Len() int      { return len(a) }
func (a Stats) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Stats) Less(i, j int) bool {
	return a[i].Count > a[j].Count || (a[i].Count == a[j].Count && a[i].Hood < a[j].Hood)
}
