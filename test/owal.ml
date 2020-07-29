open Lwt.Infix
open Odbutils.Owal

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

let test_file = "test.tmp"

let test_seq = [1; 5; 2; 4]

let op_seq = test_seq |> List.map (fun i -> T_p.Write i) |> List.rev

let init switch () =
  Lwt_switch.add_hook_or_exec (Some switch) (fun () ->
      Lwt_unix.unlink test_file)

let test_change_sync_reload _ () =
  T.of_file test_file
  >>= fun t ->
  let t = List.fold_left (fun t op -> T.change op t) t op_seq in
  Alcotest.(check @@ list int) "Apply ops to t" test_seq (T.get_underlying t) ;
  T.sync t >|= Result.get_ok
  >>= fun () ->
  T.close t
  >>= fun () ->
  T.of_file test_file
  >>= fun t ->
  Alcotest.(check @@ list int) "reload from file" test_seq (T.get_underlying t) ;
  T.close t

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
