package mapreduce

type reduceReader struct {
	Reader

	firstRow bool
	end      bool
}

func (r *reduceReader) Next() bool {
	if r.firstRow {
		r.firstRow = false
		return true
	}

	r.end = !r.Reader.Next()
	if r.end {
		return false
	}

	if r.KeySwitch() {
		return false
	}

	return true
}

// GroupKeys groups rows in r by key, according to KeySwitch().
//
// onKey invoked once for every distinct value of the key.
//
//	func (*myJob) Do(ctx JobContext, in Reader, out []Writer) error {
//	    return GroupKeys(in, func(in Reader) error {
//	        for in.Next() {
//	            // do stuff
//	        }
//	        return nil
//	    })
//	}
func GroupKeys(r Reader, onKey func(r Reader) error) error {
	rr := reduceReader{Reader: r}

	rr.end = !r.Next()
	for !rr.end {
		rr.firstRow = true
		if err := onKey(&rr); err != nil {
			return err
		}
	}

	return nil
}
