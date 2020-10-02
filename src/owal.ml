open! Core
open! Async
let src = Logs.Src.create "Owal"

module Log = (val Logs.src_log src : Logs.LOG)

module type Persistable = sig
  type t

  val init : unit -> t

  type op [@@deriving bin_io]

  val apply : t -> op -> t
end

module Persistant (P : Persistable) = struct
  type t = {
    writer : Async.Writer.t
  }

  let of_path path =
    let rec read_loop t reader =
      match%bind Reader.read_bin_prot reader P.bin_reader_op with
      | `Eof -> return t
      | `Ok op -> read_loop (P.apply t op) reader

    in 
    let%bind t = 
      match%map try_with (fun () -> Reader.with_file path ~f:(read_loop @@ P.init ())) with
      | Ok v -> v
      | Error _ -> P.init()
    in
    let%bind writer = Writer.open_file path in
    return ({writer}, t)

  let write t op =
    Writer.write_bin_prot t.writer P.bin_writer_op op

  let datasync t =
    Writer.fdatasync t.writer

  let close t =
    Writer.close t.writer
end

module T_p = struct 
  type t = int list

  let init () = []

  type op = Write of int [@@deriving bin_io]

  let apply t (Write i) = i :: t
end 

module T = Persistant (T_p)

let unlink path =
  match%bind Sys.file_exists path with
  | `Yes -> Unix.unlink path
  | _ -> return ()

let%expect_test "persist data" =
  let open T in
  let path = "test.wal" in
  let%bind () = unlink path in
  let%bind wal, _ = of_path path in
  List.iter [1;2;3;4] ~f:(fun i -> write wal (Write i));   
  let%bind () = close wal in
  let%bind wal, t = of_path path in
  let%bind () = close wal in
  [%sexp_of: int list] t |> Sexp.to_string_hum |> print_endline;
  [%expect {| (4 3 2 1) |}]
