## High Level API

1. `tachyon_open( dir: String )` -> opens a connection to the database where the database is stored in `dir` (where `dir` is relative to root)

2. `tachyon_prepare( query: String ) -> TachyonStatement` -> Converts query into byte-code (to be used in our VM)

3. `tachyon_step( statement: TachyonStatement )` -> Runs the tachyon statement either until completion, or we return a row of data

4. `tachyon_finalize( statement: TachyonStatement )` -> Marks the prepared statement as complete. (Might not need this, maybe we could do something in the destructor of the object)

5. `tachyon_close(  )`