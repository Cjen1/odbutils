open! Core
open! Async

type t = bytes [@@deriving bin_io]

let test i () =
  let%bind writer = Writer.open_file ~append:false "test.tmp" in
  let msg = "test_str" |> Bytes.of_string in
  let iter () =
    Writer.write_bin_prot writer bin_writer_t msg;
    Writer.fdatasync writer
  in 
  let%bind () = Unix.ftruncate (Writer.fd writer) ~len:(Int64.of_int 65535) in
  Deferred.for_ 0 ~to_:i ~do_:(fun i ->
      let st = Time.now() in
      let%bind () = iter () in
      let ed = Time.now() in
      print_endline (Fmt.str "Iteration %d took %f ms" i (Time.diff ed st |> Time.Span.to_ms));
      return ()
    )

let () =
  Command.async_spec ~summary:""
    Command.Spec.(
      empty
      +> flag "-n" ~doc:" Output file" (optional_with_default 2 int))
    test
  |> Command.run
