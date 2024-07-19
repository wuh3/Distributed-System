package util

import (
	"sync"
)

type ConcurrencyControl struct {
	semaphore  chan struct{}
	writeQueue chan struct{}
	mu         sync.Mutex
}

func NewConcurrencyControl() *ConcurrencyControl {
	cc := &ConcurrencyControl{
		semaphore:  make(chan struct{}, 1),
		writeQueue: make(chan struct{}, 4),
	}
	cc.semaphore <- struct{}{} // Initialize the write semaphore
	return cc
}

func (c *ConcurrencyControl) StartWrite() {

	// Wait for the write semaphore to be available.
	<-c.semaphore
	// Block any new reads from starting.
	c.writeQueue <- struct{}{}
	if len(c.writeQueue) == 4 {
		c.semaphore <- struct{}{}
		if len(c.writeQueue) > 0 {
			<-c.writeQueue
		}
	}
}

func (c *ConcurrencyControl) EndWrite() {
	// Allow reads to start again.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writeQueue) > 0 {
		<-c.writeQueue
	}

	// Release the write semaphore.
	if len(c.semaphore) < cap(c.semaphore) {
		c.semaphore <- struct{}{}
	}
}

func (c *ConcurrencyControl) StartRead() {
	<-c.semaphore
}

func (c *ConcurrencyControl) EndRead() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.semaphore) < cap(c.semaphore) {
		c.semaphore <- struct{}{}
	}
	for i := 0; i < len(c.writeQueue); i++ {
		<-c.writeQueue
	}
}
