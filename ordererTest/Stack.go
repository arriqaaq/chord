package main

import (
	"errors"
	bm "github.com/zebra-uestc/chord/models/bridge"
)

type Stack struct {
	data []*bm.Block
	len  int
}

func (stack Stack) Len() int {
	return stack.len
}

func (stack Stack) IsEmpty() bool {
	return stack.len == 0
}

func (stack *Stack) Push(value *bm.Block) {
	stack.data = append(stack.data, value)
}

func (stack Stack) Top() (*bm.Block, error) {
	if stack.len == 0 {
		return nil, errors.New("Out of index, len is 0")
	}
	return stack.data[stack.len-1], nil
}

func (stack *Stack) Pop() (*bm.Block, error) {
	theStack := stack.len
	if theStack == 0 {
		return nil, errors.New("Out of index, len is 0")
	}
	value := stack.data[theStack-1]
	stack.data = stack.data[:theStack-1]
	return value, nil
}
