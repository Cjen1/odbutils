open Lwt.Infix
open Odbutils.Owal

let src = Logs.Src.create "Bench"

module Log = (val Logs.src_log src : Logs.LOG)

let remove path = if Sys.file_exists path then 
    let path = Fpath.of_string path |> Result.get_ok in
    Bos.OS.Dir.delete ~must_exist:false ~recurse:true path|> Result.get_ok else ()


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
  (Fmt.pr "Setting up throughput test\n") ;
  T.of_dir test_file
  >>= fun (t, _t_wal) ->
  let stream = List.init n (fun _ -> Random.int 100) |> Lwt_stream.of_list in
  let result_q = Queue.create () in
  let test () =
    Lwt_stream.iter_s
      (fun v ->
        let start = Unix.gettimeofday () in
        T.write t (T_p.Write v) |> Lwt.ignore_result;
        T.datasync t >>= fun () ->
        let lat = Unix.gettimeofday () -. start in
        Queue.add lat result_q ; 
        Lwt.return ())
      stream
    >>= fun _ -> Lwt.return_unit
  in
  (Fmt.pr "Starting throughput test\n") ;
  time_it test
  >>= fun time ->
  (Fmt.pr "Finished throughput test!\n") ;
  let throughput = Float.(of_int n /. time) in
  remove test_file ;
  Lwt.return
    { throughput
    ; latencies= Queue.fold (fun ls e -> e :: ls) [] result_q |> Array.of_list
    }

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
  let res =
    try Lwt_main.run (throughput 10000) with e -> remove test_file ; raise e
  in
  Fmt.pr "%a\n" pp_stats res
