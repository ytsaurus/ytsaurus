package yt

func ExampleTableReader() {
	var r TableReader

	_ = func() error {
		// Always close reader to release associated resources.
		defer r.Close()

		var testRow struct {
			Key, Value int
		}

		for r.Next() {
			if err := r.Scan(&testRow); err != nil {
				return err
			}

			// Process row
		}

		// Always check reader Err().
		if r.Err() != nil {
			return r.Err()
		}

		return nil
	}()
}
