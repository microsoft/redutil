package worker

import "reflect"

// Concatenates the output from several error channels into a single one.
// Stops and closes the resulting channel when all its inputs are closed.
func concatErrs(errs ...<-chan error) <-chan error {
	cases := make([]reflect.SelectCase, len(chans))
	for i, ch := range errs {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	out := make(chan error)
	go func() {
		for len(cases) > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				cases = append(cases[:chosen], cases[chosen+1:]...)
			} else {
				out <- value.Interface().(error)
			}
		}

		close(out)
	}()

	return out
}
