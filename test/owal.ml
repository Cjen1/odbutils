open Lwt.Infix
open Odbutils.Owal

let src = Logs.Src.create "Owal_test"

module Log = (val Logs.src_log src : Logs.LOG)

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

let test_dir = "test.wal"

let test_seq = [1; 5; 2; 4]

let op_seq = test_seq |> List.map (fun i -> T_p.Write i) |> List.rev

let test_change_sync_reload switch () =
  Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
      let path = Fpath.of_string test_dir |> Result.get_ok in
      Bos.OS.Dir.delete ~must_exist:false ~recurse:true path |> Result.get_ok ;
      Lwt.return_unit)
  >>= fun () ->
  Log.debug (fun m -> m "opening file") ;
  T.of_dir ~default_file_size:28 ~batch_size:1 test_dir
  >>= fun (t_wal, t) ->
  Log.debug (fun m -> m "opened file, applying ops") ;
  let t = List.fold_left T_p.apply t op_seq in
  Alcotest.(check @@ list int) "reload from file" test_seq t ;
  Log.debug (fun m -> m "Writing ops") ;
  List.iter
    (fun (T_p.Write i as op) ->
      Log.debug (fun m -> m "Writing %d" i) ;
      T.write t_wal op |> Lwt.ignore_result)
    op_seq ;
  Log.debug (fun m -> m "datasync") ;
  T.datasync t_wal
  >>= fun () ->
  Log.debug (fun m -> m "Closing file") ;
  T.close t_wal
  >>= fun () ->
  T.of_dir ~default_file_size:28 test_dir
  >>= fun (t_wal, t') ->
  Alcotest.(check @@ list int) "reload from file" t t' ;
  T.close t_wal

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

let () =
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ;
  let _ =
    Sys.signal Sys.sigpipe
      (Sys.Signal_handle
         (fun _ -> raise @@ Unix.Unix_error (Unix.EPIPE, "", "")))
  in
  let open Alcotest_lwt in
  Lwt_main.run
  @@ run "Persistant test"
       [ ( "Basic functionality"
         , [test_case "Change and reload" `Quick test_change_sync_reload] ) ]
