package mapreduce

import (
	"encoding/json"
	// "fmt"
	"log"
	"os"
	// "io/ioutil"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// reduce 的工作，接收所有map发过来的文件，先对 相同的key值进行合并，调用 reduceF
	// 然后写入磁盘

	// 读取，解码，合并key
	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		reduceFileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(reduceFileName)
		if err != nil {
			log.Fatal("reduceFile: ", err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			// 此处用hashmap的方式合并相同key,当然也可以用排序的方式
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		file.Close()
	}

	// fmt.Println("kvMap:", len(kvMap))
	// fmt.Println("outFile:", outFile)
	// reduceF
	file, err := os.OpenFile(outFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Fatal("outFile: ", err)
	}
	enc := json.NewEncoder(file)
	for k, v := range kvMap {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}

	file.Close()

}
