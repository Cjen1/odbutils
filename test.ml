open! Core
open Owal
open Async

module T_p = struct 
  type t = int list

  let init () = []

  type op = Write of int [@@deriving bin_io]

  let apply t (Write i) = i :: t
end 

module T = Persistant (T_p)
let unlink path =
  let open Async in
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
  let%bind () = [%expect {| (4 3 2 1) |}] in
  unlink path
