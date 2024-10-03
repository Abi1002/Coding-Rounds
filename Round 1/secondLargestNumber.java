package main
import (
"fmt"
)
func findSecondLargest(arr []int) (int, error) {
if len(arr) < 2 {
return 0, fmt.Errorf("Atleast two elements needed !")
}
largest := arr[0]
secondLargest := arr[0]
for _, value := range arr {
if value > largest {
largest = value
}
}
found := false
for _, value := range arr {
if value > secondLargest && value < largest {
secondLargest = value
found = true
}
}
if !found {
return 0, fmt.Errorf("second largest element Not found")
}
return secondLargest, nil
}
func main() {
arr := []int{10, 20, 4, 45, 99, 33}
secondLargest, err := findSecondLargest(arr)
if err != nil {
fmt.Println(err)
} else {
fmt.Printf("The second largest element: %d\n", secondLargest)
}
}