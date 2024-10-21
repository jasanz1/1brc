import argv
import gleam/int
import file_streams/file_stream
import file_streams/file_stream_error
import gleam/float
import gleam/io
import gleam/list
import gleam/otp/task
import gleam/result
import gleam/string

pub type WeatherStation {
  Station(
    city: String,
    min: Float,
    max: Float,
    total: Float,
    count: Float,
    avg: Float,
  )
  Entry(city: String, temp: Float)
}

pub fn main() {
  let assert Ok(file_name) = case argv.load().arguments {
    [file_name] -> Ok(file_name)
    _ -> Error(Nil)
  }
  let assert Ok(file) = file_stream.open_read(file_name)
  let result =
    work(file,0)
    |> list.fold([], fn(stations, line) {
      apply_station(line, stations, combine_stations)
    })
    |> list.map(fn(x) {
      case  x {
        Station(city: city, min: min, max: max, total: total, count: count, avg: _) -> Station( city: city, min: min, max: max, total: total, count: count, avg: total /. count,)
        _ -> x
      }
    })
  result
  |> list.sort(fn(a, b) { string.compare(a.city, b.city) })
  |> list.map(fn(station) {
    let assert Ok(_) = case station {
      Station(city: city, min: min, max: max, total: _, count: _, avg: avg) -> {
        city
        |> string.append("=")
        |> string.append(
          { { min *. 10.0 } |> float.floor } /. 10.0 |> float.to_string,
        )
        |> string.append("/")
        |> string.append(
          { { avg *. 10.0 } |> float.floor } /. 10.0 |> float.to_string,
        )
        |> string.append("/")
        |> string.append(
          { { max *. 10.0 } |> float.floor } /. 10.0 |> float.to_string,
        ) |> io.debug |> Ok
      }
      _ -> Error(Nil)
    }
  })
}
fn work(file,count) {
  let chunk_size = 1_000_000
  io.println("chunks " <> count |> int.to_string <> " percent:" <> {chunk_size /1_000_000_000} |> int.to_string)
  let assert Ok(pos) = file_stream.position(file,file_stream.CurrentLocation(0))
  let assert Ok(station) = case
    file_stream.read_list(file, file_stream.read_line, chunk_size)
  {
    Ok(lines) -> {
      let work_task = task.async(fn() { do_work(lines) })
      let other_work =  work(file,count+chunk_size)
      let finished_work = task.await_forever(work_task)
      let return = [finished_work,other_work] |> list.flatten |> Ok
      return
    }
    Error(file_stream_error.Eof) -> {
      
      let assert Ok(_) = file_stream.position(file,file_stream.BeginningOfFile(pos))
      let assert Ok(rest) = file |> read_rest
      rest |> do_work |> Ok
    }
    _ -> Error(Nil)
  }
  station
}

fn do_work(lines) {

  lines
  |> list.map(math)
  |> list.fold([], fn(stations, line) {
    apply_station(line, stations, update_station)
  })
}


fn apply_station(
  line: WeatherStation,
  weather_stations: List(WeatherStation),
  update: fn(WeatherStation, WeatherStation) -> WeatherStation,
) {
  let assert Ok(station) = case weather_stations, line{
    [s, ..] ,_-> Ok(s)
    [], Entry(city: city, temp: temp) ->
      Station(
        city:city,
        min: temp,
        max: temp,
        total: 0.0,
        count: 1.0,
        avg: 0.0,
      )|> Ok
      [], Station(city: city, min: min, max: max, total: _, count: _, avg: _) ->
      Station(
        city:city,
      min: min,
      max: max,
      total: 0.0,
      count: 1.0,
      avg: 0.0,
    ) |> Ok
  }
   case line.city == station.city {
    True -> [ update(line, station), ..list.rest(weather_stations) |> result.unwrap([]) ]
    False -> [station,..apply_station(line,weather_stations |> list.rest |> result.unwrap([]),update)]
  }
}

fn combine_stations(
  station: WeatherStation,
  weather_stations: WeatherStation,
) -> WeatherStation {
  let assert Ok(return) = case station, weather_stations{
  Station(city: city, min: min, max: max, total: total, count: count, avg: _),  Station(city: _, min: w_min, max: w_max, total: w_total, count: w_count, avg: w_avg) -> 
  Station(
    city: city,
    min: float.min(w_min, min),
    max: float.max(w_max, max),
    total: w_total +. total,
    count: w_count +. count,
    avg: w_avg,
  ) |> Ok
    _,_ -> Error(Nil)
    }
    return
}


fn update_station(
  station: WeatherStation,
  weather_stations: WeatherStation,
) -> WeatherStation {
  let assert Ok(return) = case station, weather_stations{
  Entry(city: city, temp: temp), Station(city: _, min: w_min, max: w_max, total: w_total, count: w_count, avg: w_avg) -> Station(
    city: city,
    min: float.min(w_min, temp),
    max: float.max(w_max, temp),
    total: w_total +. temp,
    count: w_count +. 1.0,
    avg: w_avg,
  ) |> Ok
  _,_ -> Error(Nil)
  }
  return
}

fn math(line) {

  line
  |> string.split(";")
  |> fn(x) {
    let assert Ok(#(city,temp)) = case x{
      [c,t] -> Ok(#(c,t))
      _ -> Error(Nil)
    }
    let assert Ok(temp) = temp
      |> string.trim
      |> float.parse
    Entry(city: city, temp: temp)
  }
}

fn read_rest(file) -> Result(List(String), Nil) {
  let line = file_stream.read_line(file)  
  case line{
    Ok(line) -> {
      let assert Ok(rest) = read_rest(file)
      Ok([line, ..rest])
    }
    Error(file_stream_error.Eof) -> Ok([])
    _ -> Error(Nil)
  }
}
