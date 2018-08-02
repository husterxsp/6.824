package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

// func DPrint(args ...interface{}) {
// 	ret := ""
// 	for _, v := range args {
// 		switch v.(type) {
// 		case int:
// 			ret += v.(int)
// 		case string:
// 			ret += v.(string)
// 		default:
// 			fmt.Printf("I don't know about type %T!\n", v)
// 		}
// 	}
// 	if Debug > 0 {
// 		log.Printf(ret)
// 	}
// }

func DPrint(args ...interface{}) {
	if Debug > 0 {
		Info := log.New(os.Stdout, "", log.Lmicroseconds)
		Info.Println(args)
	}
}
