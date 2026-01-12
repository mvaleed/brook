package main

import "fmt"

func main() {
	mySlice := make([]int, 4, 5)
	fmt.Println(mySlice)
	change(mySlice)
	fmt.Println(mySlice)
}

func change(s []int) {
	s = append(s, 1, 2)
	s[3] = 10
}
