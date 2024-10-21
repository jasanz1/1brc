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
  WeatherStation(
    city: String,
    min: Float,
    max: Float,
    total: Float,
    count: Float,
    avg: Float,
  )
}

pub type Entry {
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
      wapply_station(line, stations, combine_stations)
    })
    |> list.map(fn(x) {
      WeatherStation(
        city: x.city,
        min: x.min,
        max: x.max,
        total: x.total,
        count: x.count,
        avg: x.total /. x.count,
      )
    })
  result
  |> list.sort(fn(a, b) { string.compare(a.city, b.city) })
  |> list.map(fn(station) {
    station.city
    |> string.append("=")
    |> string.append(
      { { station.min *. 10.0 } |> float.floor } /. 10.0 |> float.to_string,
    )
    |> string.append("/")
    |> string.append(
      { { station.avg *. 10.0 } |> float.floor } /. 10.0 |> float.to_string,
    )
    |> string.append("/")
    |> string.append(
      { { station.max *. 10.0 } |> float.floor } /. 10.0 |> float.to_string,
    )
  })
  |> io.debug
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

fn wapply_station(
  line: WeatherStation,
  weather_stations: List(WeatherStation),
  update: fn(WeatherStation, WeatherStation) -> WeatherStation,
) {
  let station = case weather_stations{
    [s, ..] -> s 
    [] ->  
      WeatherStation(
        city: line.city,
        min: line.min,
        max: line.max,
        total: 0.0,
        count: 1.0,
        avg: 0.0,
      )
  }

  case line.city == station.city {
    True -> [ update(line, station), ..list.rest(weather_stations) |> result.unwrap([]) ]
    False -> [station, ..wapply_station(line,weather_stations |> list.rest |> result.unwrap([]),update)]
  }
}

fn apply_station(
  line: Entry,
  weather_stations: List(WeatherStation),
  update: fn(Entry, WeatherStation) -> WeatherStation,
) {
  let station = case weather_stations{
    [s, ..] -> s 
    [] ->  
      WeatherStation(
        city: line.city,
        min: line.temp,
        max: line.temp,
        total: 0.0,
        count: 1.0,
        avg: 0.0,
      )
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
  WeatherStation(
    city: station.city,
    min: float.min(weather_stations.min, station.min),
    max: float.max(weather_stations.max, station.max),
    total: weather_stations.total +. station.total,
    count: weather_stations.count +. station.count,
    avg: weather_stations.avg,
  )
}

fn update_station(
  station: Entry,
  weather_stations: WeatherStation,
) -> WeatherStation {
  WeatherStation(
    city: station.city,
    min: float.min(weather_stations.min, station.temp),
    max: float.max(weather_stations.max, station.temp),
    total: weather_stations.total +. station.temp,
    count: weather_stations.count +. 1.0,
    avg: weather_stations.avg,
  )
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
