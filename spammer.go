package main

import (
	"fmt"
	"log"
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
		value, ok := v.(string)
		if !ok {
			log.Printf("select users: не удалось привести к string: %v", v)
			return
		}
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
		}(value, out)
	}

	wg.Wait()
}

func SelectMessagesWorker(users []User, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	msg, _ := GetMessages(users...)

	for _, value := range msg {
		out <- value
	}
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup
	users := make([]User, 0, GetMessagesMaxUsersBatch)
	for v := range in {
		users = append(users, v.(User))
		if len(users) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go SelectMessagesWorker(users, out, &wg)
			users = make([]User, 0, GetMessagesMaxUsersBatch)
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
		value, ok := msg.(MsgID)
		if !ok {
			log.Printf("worker: не удалось привести к msgID: %v", msg)
			return
		}
		temp, _ := HasSpam(value)
		out <- CheckSpamStruct{MsgID: value, HasSpam: temp}
	}
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup
	for i := 0; i < HasSpamMaxAsyncRequests; i++ {
		wg.Add(1)
		go worker(in, out, &wg)
	}
	wg.Wait()

}

func CombineResults(in, out chan interface{}) {
	total := []CheckSpamStruct{}
	for v := range in {
		value, ok := v.(CheckSpamStruct)
		if !ok {
			log.Printf("combine results: не удалось привести к CheckSpamStruct: %v", v)
			return
		}
		total = append(total, value)
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
