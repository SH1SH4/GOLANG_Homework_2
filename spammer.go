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
	seen := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for v := range in {
		wg.Add(1)
		go func(email string, out chan interface{}) {
			defer wg.Done()
			user := GetUser(email)
			mu.Lock()
			if seen[user.Email] {
				mu.Unlock()
				return
			}
			seen[user.Email] = true
			mu.Unlock()
			out <- user
		}(v.(string), out)
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup
	users := []User{}
	for {
		v, isOpen := <-in
		if len(users) == 2 || !isOpen {
			wg.Add(1)
			go func(users []User, out chan interface{}) {
				msg, _ := GetMessages(users...)
				for _, value := range msg {
					out <- value
				}
				wg.Done()
			}(users, out)
			users = []User{}
		}
		if !isOpen {
			break
		}
		users = append(users, v.(User))
	}

	wg.Wait()
}

type CheckSpamStruct struct {
	MsgID   MsgID
	HasSpam bool
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup
	numWorkers := 5
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(in, out chan interface{}) {
			defer wg.Done()
			for msg := range in {
				temp, _ := HasSpam(msg.(MsgID))
				out <- CheckSpamStruct{MsgID: msg.(MsgID), HasSpam: temp}
			}
		}(in, out)
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
