1. The reading and writing of metadata needs to be distinguished from the reading and writing of data (chunks) to tables and files.
2. From a master's point of view, data reads/writes look like the following sequence of commands:
   * Reading:
      * GetBasicAttributes: Getting some service attributes necessary for reading.
      * Fetch: Getting a list of chunks that make up the file or table.
   * Writing:
      * GetBasicAttributes: Getting some service attributes necessary for writing.
      * BeginUpload: Starting the upload transaction.
      * EndUpload: Completing the upload transaction.
3. When reading/writing data, the GetBasicAttributes command targets one cell, while Fetch, BeginUpload, and EndUpload target another — this is normal.
4. In most cases, copying or moving a table looks like the Copy or Move commands. The BeginCopy and EndCopy commands are used when copying/moving crosses the Cypress sharding boundaries. In practice, such cases are rare.
5. Dynamic table requests (insert-rows, select-rows, lookup-rows, and other data operations) are **not recorded** in the table access history. The log only captures requests to the Cypress master (metadata and static table operations); requests to tablet nodes bypass the master and are not included in the log.