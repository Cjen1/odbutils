open Lwt.Infix

let src = Logs.Src.create "Owal"

module Log = (val Logs.src_log src : Logs.LOG)

module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) = struct
  include P

  type t =
    { t: P.t
    ; batcher: (int * (bytes -> offset:int -> unit)) Batcher.t
    ; batch_fn : (int * (bytes -> offset:int -> unit)) list -> unit Lwt.t
    ; fd: Lwt_unix.file_descr
    ; prev: unit Lwt.t ref }

  let write_batch prev_ref fd = function
    | [] -> 
      Log.debug (fun m -> m "Empty batch");
      Lwt.return_unit
    | bs ->
    let base_write fd bs =
      let total_size =
        List.fold_left (fun acc (p_len, _p_blit) -> acc + p_len + 8) 0 bs
      in
      let buf = Bytes.create total_size in
      let size =
        List.fold_left
          (fun offset (p_len, p_blit) ->
            Log.debug (fun m -> m "Blitting message, p_len:%d" p_len) ;
            EndianBytes.LittleEndian.set_int64 buf offset (Int64.of_int p_len) ;
            p_blit buf ~offset:(offset + 8) ;
            offset + p_len + 8)
          0 bs
      in
      assert (size = total_size) ;
      let rec loop offset size =
        Lwt_unix.write fd buf offset size
        >>= fun written ->
        if written = size then Lwt.return_unit
        else loop (offset + written) (size - written)
      in
      Log.debug (fun m -> m "Writing to disk");
      loop 0 size
    in
    match Lwt.state !prev_ref with
    | Lwt.Return () ->
        let p = base_write fd bs in
        prev_ref := p ;
        p
    | Lwt.Sleep ->
        let p = !prev_ref >>= fun () -> base_write fd bs in
        prev_ref := p ;
        p
    | Lwt.Fail exn ->
        Lwt.fail exn

        (*
  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let p = Batcher.auto_dispatch t.batcher (p_len, p_blit) t.batch_fn in
    Lwt.on_failure p !Lwt.async_exception_hook ;
    t
    *)

  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let b_len = p_len + 8 in
    let buf = Bytes.create b_len in
    EndianBytes.LittleEndian.set_int64 buf 0 (Int64.of_int p_len);
    p_blit buf ~offset:8;
    Log.debug(fun m -> m "Start _write");
    assert(Unix.write (Lwt_unix.unix_file_descr t.fd) buf 0 b_len = b_len);
    Log.debug(fun m -> m "End _write");
    t

  let sync t =
    (*
    Log.debug(fun m -> m "Start batch_write");
    Batcher.perform t.batcher t.batch_fn
    >>= fun () ->
    Log.debug(fun m -> m "End batch_write");
       *)
    Log.debug(fun m -> m "Start fdatasync");
    Lwt_unix.fdatasync t.fd
    >>= fun () ->
    Log.debug(fun m -> m "End fdatasync");
    (* All pending writes now complete *)
    Logs.debug (fun m -> m "Finished syncing") ;
    Lwt.return_ok ()

  let do_one_cycle t buf =
    let size = Bytes.length buf in
    Lwt_unix.write t.fd buf 0 size >>= fun written ->
    assert(written = size);
    Lwt_unix.fdatasync t.fd

  let read_value channel =
    let rd_buf = Bytes.create 8 in
    Lwt_io.read_into_exactly channel rd_buf 0 8
    >>= fun () ->
    let size = EndianBytes.LittleEndian.get_int64 rd_buf 0 |> Int64.to_int in
    Log.debug (fun m -> m "Reading payload of %a bytes" Fmt.int size) ;
    let payload_buf = Bytes.create size in
    Lwt_io.read_into_exactly channel payload_buf 0 size
    >>= fun () -> payload_buf |> Lwt.return

  (* Store wal in a directory filled with files, go in ascending order from 1.wal
     Open file, ftruncate it to 1mb min then start writing to it as normal
     When next update wouldn't fit into file, create new file which can accommodate the next update
        truncate old file to ensure end doesn't have 0s
   *)
  let of_file path =
    Logs.debug (fun m -> m "Trying to open file") ;
    Lwt_unix.openfile path Lwt_unix.[O_RDONLY; O_CREAT] 0o640
    >>= fun fd ->
    let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input fd in
    let stream =
      Lwt_stream.from (fun () ->
          Lwt.catch
            (fun () ->
              read_value input_channel
              >>= fun v -> decode v ~offset:0 |> Lwt.return_some)
            (function _ -> Lwt.return_none))
    in
    Logs.debug (fun m -> m "Reading in") ;
    Lwt_stream.fold
      (fun v t ->
        Log.debug (fun m -> m "Applying op") ;
        P.apply t v)
      stream (P.init ())
    >>= fun t ->
    Lwt_io.close input_channel
    >>= fun () ->
    Logs.debug (fun m -> m "Creating fd for persistance") ;
    Lwt_unix.openfile path Lwt_unix.[O_WRONLY] 0o666
    >>= fun fd ->
    Lwt_unix.lseek fd 0 Lwt_unix.SEEK_END >>= fun curr_loc ->
    assert(curr_loc >= 0);
    Lwt_unix.ftruncate fd (1000*1000)>>= fun () ->
    Lwt_unix.fsync fd >>= fun () ->
    let prev = ref Lwt.return_unit in
    let batch_fn = write_batch prev fd in
    let batcher = Batcher.create 10 in
    Lwt.return {t; batcher; batch_fn; fd; prev}

  let change op t = {(write t op) with t= P.apply t.t op}

  let close t =
    sync t
    >>= function
    | Error exn ->
        Log.err (fun m -> m "Error on closing batch %a" Fmt.exn exn) ;
        Lwt.return_unit
    | Ok () ->
        Lwt_unix.fsync t.fd
        >>= fun () ->
        Lwt_unix.close t.fd
        >>= fun () ->
        Log.info (fun m -> m "Closed wal successfully") ;
        Lwt.return_unit

  let get_underlying t = t.t
end
