package blobtable

import "io/ioutil"

func ExampleBlobTableReader() {
	var r BlobTableReader

	_ = func() error {
		// Always close reader to release associated resources.
		defer r.Close()

		for r.Next() {
			blob, err := ioutil.ReadAll(r)
			if err != nil {
				return err
			}

			_ = blob
			// Process blob
		}

		// Always check reader Err().
		if r.Err() != nil {
			return r.Err()
		}

		return nil
	}()
}
