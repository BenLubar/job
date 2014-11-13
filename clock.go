package job

import "time"

// Clock sends the value given to the Send function after the delay specified in
// NewClock. Sending another value will restart the timer.
type Clock struct {
	C  <-chan interface{}
	c  chan<- interface{} // == C
	in chan interface{}   // != C
}

// NewClock constructs a new Clock with the given delay. Call Close to stop the
// clock's goroutine.
func NewClock(d time.Duration) *Clock {
	ch := make(chan interface{})
	in := make(chan interface{}, 1)
	c := &Clock{
		C:  ch,
		c:  ch,
		in: in,
	}
	go c.loop(d)
	return c
}

// Send restarts the timer with the new value. Start will panic if Close has been
// called.
func (c *Clock) Send(v interface{}) {
	for {
		select {
		case c.in <- v:
			return

		case <-c.in:
			// discard
		}
	}
}

// Close stops the timer. Close will panic if Close has already been called.
func (c *Clock) Close() {
	close(c.in)
}

func (c *Clock) loop(d time.Duration) {
	defer close(c.c)

	var v interface{}
	var have bool

	for {
		var after <-chan time.Time

		if have {
			after = time.After(d)
		}

		select {
		case in, ok := <-c.in:
			if !ok {
				return
			}
			have, v = true, in

		case <-after:
			select {
			case c.c <- v:
				have, v = false, nil

			case in, ok := <-c.in:
				if !ok {
					return
				}
				v = in
			}
		}
	}
}
