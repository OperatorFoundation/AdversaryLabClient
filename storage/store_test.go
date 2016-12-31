package storage

import "testing"

// func TestAdd(t *testing.T) {
// 	runtime.GOMAXPROCS(runtime.NumCPU())
//
// 	store, err := OpenStore("testing")
// 	if err != nil {
// 		fmt.Println("Error opening store")
// 		fmt.Println(err)
// 		t.Error(err)
// 	}
//
// 	for i := 0; i < 100; i++ {
// 		bs := make([]byte, 100)
// 		store.Add(bs)
// 	}
//
// 	store.Close()
//
// 	store, err = OpenStore("testing")
// 	if err != nil {
// 		fmt.Println("Error opening store")
// 		fmt.Println(err)
// 		t.Error(err)
// 	}
// }

func TestBytemap(t *testing.T) {
	t.Log("TestBytemap")
	// bytemap, err := NewBytemap("HTTP-testing")
	// t.Log("Loaded")
	// if err != nil {
	// 	t.Error("Error opening bytemap file " + err.Error())
	// }
	//
	// counter := 0
	// for i := 0; i < 1500; i++ {
	// 	for j := 0; j < 256; j++ {
	// 		for k := 0; k < 256; k++ {
	// 			t.Log("->")
	// 			bytemap.PutCount(i, byte(j), byte(k), int64(counter))
	// 			counter++
	// 		}
	// 	}
	// }
	//
	// counter = 0
	// for i := 0; i < 1500; i++ {
	// 	for j := 0; j < 256; j++ {
	// 		for k := 0; k < 256; k++ {
	// 			t.Log("<-")
	// 			observed := bytemap.GetCount(i, byte(j), byte(k))
	// 			if observed != int64(counter) {
	// 				t.Fail()
	// 			}
	// 			counter++
	// 		}
	// 	}
	// }
}
