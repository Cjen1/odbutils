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

let main () = 
  let count = 1000 in
  let file = "bench.dat" in
  let size = 8 in

  let buf = Bytes.create size in
  let () = 
    let fd = Unix.openfile "/dev/urandom" [Unix.O_RDONLY] 0o666 in
    assert (Unix.read fd buf 0 size = size);
    Unix.close fd
  in 
  (*
  let fd = 
      Unix.openfile file Unix.[O_WRONLY; O_CREAT; O_TRUNC] 0o666
  in
  Unix.LargeFile.ftruncate fd (size * count |> Int64.of_int);
  Unix.fsync fd;
  let duration =
      let fd = Lwt_unix.of_unix_file_descr fd in
     *)
  if Sys.file_exists file then Unix.unlink file;
  let run () =
    T.of_file file >>= fun t -> 
    let rec loop = function
      | 0 -> 
        Lwt.return_unit
      | count ->
        T.do_one_cycle t buf
        >>= fun () ->
        loop (count - 1)
    in 
    let start = Unix.gettimeofday () in
    loop count >|= fun () ->
    (Unix.gettimeofday () -. start)
  in
  let duration = Lwt_main.run (run ()) in
  Fmt.pr "Took %.3fms per write" ((duration /. Float.of_int count) *. 1000.)

let () = main ()
