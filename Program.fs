module TestTimestamps.Program

open System
open System.Collections.Generic
open System.Threading
open System.Data
open System.Globalization
open Microsoft.Data.SqlClient
open FSharpPlus
open SqlKata.Execution
open SqlHydra.Query

let connString = "Server=your_server;Database=TimestampPrecision;Trusted_Connection=True;TrustServerCertificate=True"

Dapper.SqlMapper.AddTypeMap(typeof<DateTime>, DbType.DateTime2)

let firstTimeInDB = 
  let baseTimestamp = DateTime(2022,07,20, 15,05,45)   
  DateTime (baseTimestamp.Ticks + 1234567L)

let setTimestampsToPrintFullPrecision () = 
  let culture = CultureInfo.CurrentCulture.Clone() :?> CultureInfo
  culture.DateTimeFormat.LongTimePattern <- "HH:mm:ss.fffffff"
  Thread.CurrentThread.CurrentCulture <- culture

let runWithJustADO () = task {
  use conn = new SqlConnection(connString)
  do! conn.OpenAsync()
  let sql = "SELECT MIN([TimeCol]) FROM [TestTable] WHERE [TimeCol] > @ts"
  let cmd = new SqlCommand(sql, conn)
  let param = cmd.Parameters.Add("@ts", SqlDbType.DateTime2)
  param.Value <- firstTimeInDB
  let! dt = cmd.ExecuteScalarAsync()

  printfn "Microsoft.Data.SqlClient with DATETIME2: %A" dt
}

let runWithADOAsDateTime () = task {
  use conn = new SqlConnection(connString)
  do! conn.OpenAsync()
  let sql = "SELECT MIN([TimeCol]) FROM [TestTable] WHERE [TimeCol] > @ts"
  let cmd = new SqlCommand(sql, conn)
  let param = cmd.Parameters.Add("@ts", SqlDbType.DateTime)
  param.Value <- firstTimeInDB
  let! dt = cmd.ExecuteScalarAsync()

  printfn "Microsoft.Data.SqlClient with DATETIME: %A" dt
}


let runWithSqlKata () = task {
  let compiler = SqlKata.Compilers.SqlServerCompiler()
  use conn = new SqlConnection(connString)
  let db = new QueryFactory(conn, compiler)
  let! dt = db.Query("TestTable").AsMin("TimeCol").Where("TimeCol", ">", firstTimeInDB).FirstAsync()
  let dt = (dt :?> IDictionary<string, obj>)["min"]

  printfn "SqlKata: %A" dt
}

let runWithSqlHydra () = task {
  let openContext () = 
    let compiler = SqlKata.Compilers.SqlServerCompiler()
    let conn = new SqlConnection(connString)
    new QueryContext(conn, compiler)

  let ts = firstTimeInDB
  let! dt = 
    selectTask TimestampPrecisionDB.HydraReader.Read (ContextType.Create openContext) {
      for row in table<TimestampPrecisionDB.dbo.TestTable> do
      where (row.TimeCol > ts)
      select (minBy row.TimeCol)
    }
    |>> Seq.head

  printfn "SqlHydra: %A" dt
}

[<EntryPoint>]
let main argv = 
  setTimestampsToPrintFullPrecision ()
  let tsk = task {
    do! runWithJustADO ()
    do! runWithADOAsDateTime ()
    do! runWithSqlKata ()
    do! runWithSqlHydra ()
  }

  do tsk.Wait()

  0
