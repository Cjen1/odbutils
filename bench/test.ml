let main lwt = 
  let count = 10 in
  let file = "bench.dat" in
  let size = 8 in

  let fsync = false in

  let buf = Bytes.create size in
  let () = 
    let fd = Unix.openfile "/dev/urandom" [Unix.O_RDONLY] 0o666 in
    assert (Unix.read fd buf 0 size = size);
    Unix.close fd
  in 
  let fd = 
      Unix.openfile file Unix.[O_WRONLY; O_CREAT; O_TRUNC] 0o666
  in
  Unix.LargeFile.ftruncate fd (size * count |> Int64.of_int);
  Unix.fsync fd;
  let duration =
      let fd = Lwt_unix.of_unix_file_descr fd in
      let open Lwt.Infix in
      let rec loop = function
        | 0 -> 
          Lwt.return_unit
        | count ->
          Lwt_unix.write fd buf 0 size >>= fun written ->
          assert(written = size);
          (
            if fsync then
              Lwt_unix.fsync fd
            else
              Lwt_unix.fdatasync fd
          )
          >>= fun () ->
          loop (count - 1)
      in 
      let run () =
        let start = Unix.gettimeofday () in
        loop count >|= fun () ->
        (Unix.gettimeofday () -. start)
      in
      Lwt_main.run (run ())
  in 
  Fmt.pr "Took %.3fms per write for %s\n" ((duration /. Float.of_int count) *. 1000.) (if lwt then "lwt" else "straight") 

let () = main false; main true
