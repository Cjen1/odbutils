open Lwt.Infix
open Odbutils.Owal

let src = Logs.Src.create "Bench"

module Log = (val Logs.src_log src : Logs.LOG)

let remove path = if Sys.file_exists path then Unix.unlink path else ()

module T_p = struct
  type t = int list

  let init () = []

  type op = Write of int

  let encode_blit = function
    | Write i ->
        ( 8
        , fun buf ~offset ->
            EndianBytes.LittleEndian.set_int64 buf offset (Int64.of_int i) )

  let decode buf ~offset =
    Write (EndianBytes.LittleEndian.get_int64 buf offset |> Int64.to_int)

  let apply t (Write i) = i :: t
end

module T = Persistant (T_p)

let time_it f =
  let start = Unix.gettimeofday () in
  f () >>= fun () -> Unix.gettimeofday () -. start |> Lwt.return

let test_file = "test.tmp"

type test_res = {throughput: float; latencies: float array}

let throughput n =
  Log.info (fun m -> m "Setting up throughput test") ;
  T.of_file test_file
  >>= fun t ->
  let stream = List.init n (fun _ -> Random.int 100) |> Lwt_stream.of_list in
  let result_q = Queue.create () in
  let test () =
    Lwt_stream.fold_s
      (fun v t ->
        Lwt.pause ()
        >>= fun () ->
        let start = Unix.gettimeofday () in
        let t = T.change (T_p.Write v) t in
        T.sync t |> Lwt_result.get_exn
        >>= fun () ->
        let lat = Unix.gettimeofday () -. start in
        Queue.add lat result_q ; Lwt.return t)
      stream t
    >>= fun _ -> Lwt.return_unit
  in
  Log.info (fun m -> m "Starting throughput test") ;
  time_it test
  >>= fun time ->
  Log.info (fun m -> m "Finished throughput test!") ;
  let throughput = Base.Float.(of_int n / time) in
  remove test_file ;
  Lwt.return
    { throughput
    ; latencies= Queue.fold (fun ls e -> e :: ls) [] result_q |> Array.of_list
    }

let reporter =
  let open Core in
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

(* test_res Fmt.t*)
let pp_stats =
  let open Owl.Stats in
  let open Fmt in
  let fields =
    [ field "throughput" (fun s -> s.throughput) float
    ; field "mean" (fun s -> mean s.latencies) float
    ; field "p50" (fun stats -> percentile stats.latencies 50.) float
    ; field "p75" (fun stats -> percentile stats.latencies 75.) float
    ; field "p99" (fun stats -> percentile stats.latencies 99.) float ]
  in
  record fields

let () =
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ;
  let res =
    try Lwt_main.run (throughput 10) with e -> remove test_file ; raise e
  in
  Fmt.pr "%a" pp_stats res
