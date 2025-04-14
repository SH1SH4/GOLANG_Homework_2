package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	in := make(chan interface{})
	var wg sync.WaitGroup
	wg.Add(len(cmds))

	for _, cmd := range cmds {
		out := make(chan interface{})
		go func(cmd func(in, out chan interface{}), in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			cmd(in, out)
		}(cmd, in, out)
		in = out
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	seen := make(map[string]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup

	for v := range in {
		wg.Add(1)
		go func(email string, out chan interface{}) {
			defer wg.Done()
			user := GetUser(email)
			mu.Lock()
			if _, ok := seen[user.Email]; ok {
				mu.Unlock()
				return
			}
			seen[user.Email] = struct{}{}
			mu.Unlock()
			out <- user
		}(v.(string), out)
	}

	wg.Wait()
}

func SelectMessagesWorker(users []User, out chan interface{}, wg *sync.WaitGroup) {
	msg, _ := GetMessages(users...)

	for _, value := range msg {
		out <- value
	}
	wg.Done()
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup
	users := []User{}
	for v := range in {
		users = append(users, v.(User))
		if len(users) == 2 {
			wg.Add(1)
			go SelectMessagesWorker(users, out, &wg)
			users = []User{}
			// users = users[:0]
		}
	}
	if len(users) > 0 {
		wg.Add(1)
		go SelectMessagesWorker(users, out, &wg)
	}

	wg.Wait()
}

type CheckSpamStruct struct {
	MsgID   MsgID
	HasSpam bool
}

func worker(in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range in {
		temp, _ := HasSpam(msg.(MsgID))
		out <- CheckSpamStruct{MsgID: msg.(MsgID), HasSpam: temp}
	}
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup
	numWorkers := 5
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(in, out, &wg)
	}
	wg.Wait()

}

func CombineResults(in, out chan interface{}) {
	total := []CheckSpamStruct{}
	for value := range in {
		v := value.(CheckSpamStruct)
		total = append(total, v)
	}
	sort.Slice(total, func(i, j int) bool {
		if total[i].HasSpam != total[j].HasSpam {
			return total[i].HasSpam && !total[j].HasSpam
		}
		return total[i].MsgID < total[j].MsgID
	})
	for _, v := range total {
		out <- fmt.Sprintf("%t %v", v.HasSpam, v.MsgID)
	}
}
