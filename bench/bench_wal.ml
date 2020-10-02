(*

open Lwt.Infix
open Odbutils.Owal

let src = Logs.Src.create "Bench"

module Log = (val Logs.src_log src : Logs.LOG)

type test_res = {throughput: float; latencies: float array} [@@deriving yojson]

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

module T_p = struct
  type t = bytes list

  let init () = []

  type op = Write of bytes

  let encode_blit =
    let handler b len buf ~offset =
      EndianBytes.BigEndian_unsafe.set_int64 buf offset (Int64.of_int len) ;
      BytesLabels.blit ~src:b ~src_pos:0 ~dst:buf ~dst_pos:offset ~len
    in
    function
    | Write b ->
        let len = Bytes.length b in
        (len + 8, handler b len)

  let decode buf ~offset =
    let len =
      EndianBytes.BigEndian_unsafe.get_int64 buf offset |> Int64.to_int
    in
    let b = BytesLabels.sub buf ~pos:offset ~len in
    Write b

  let apply t (Write b) = b :: t
end

module T = Persistant (T_p)

let time_it f =
  let start = Unix.gettimeofday () in
  f () >>= fun () -> Unix.gettimeofday () -. start |> Lwt.return

let throughput file n write_size =
  Log.info (fun m -> m "Setting up throughput test\n" );
  T.of_dir file
  >>= fun (t, _t_wal) ->
  let stream =
    List.init n (fun _ -> Bytes.init write_size (fun _ -> 'c'))
    |> Lwt_stream.of_list
  in
  let result_q = Queue.create () in
  let test () =
    Lwt_stream.iter_s
      (fun v ->
        let start = Unix.gettimeofday () in
        T.write t (T_p.Write v) |> Lwt.ignore_result ;
        T.datasync t
        >>= fun () ->
        let lat = Unix.gettimeofday () -. start in
        Queue.add lat result_q ; Lwt.return ())
      stream
    >>= fun _ -> Lwt.return_unit
  in
  Log.info (fun m -> m "Starting throughput test\n" );
  time_it test
  >>= fun time ->
  Log.info (fun m -> m "Finished throughput test!\n" );
  let throughput = Float.(of_int n /. time) in
  { throughput
  ; latencies= Queue.fold (fun ls e -> e :: ls) [] result_q |> Array.of_list }
  |> Lwt.return

let reporter =
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("%a %a @[" ^^ fmt ^^ "@]@.")
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  let n = 10000 in
  let perform () =
    let write_sizes = [0; 4; 16; 64; 256; 1024; 4096; 8192; 32768] in
    let iter write_size =
      let tag = Fmt.str "test_%d.tmp" write_size in
      throughput tag n write_size
      >>= fun res ->
      Log.info (fun m -> m "%a\n" pp_stats res );
      let json = test_res_to_yojson res in
      let fpath = Fmt.str "%d.json" write_size in
      Yojson.Safe.to_file fpath json ;
      Lwt.return_unit
    in
    Lwt_list.iter_s iter write_sizes
  in
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ; 
  Lwt_main.run (perform ())
   *)
