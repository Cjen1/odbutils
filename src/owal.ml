open Lwt.Infix

let src = Logs.Src.create "Owal"

module Log = (val Logs.src_log src : Logs.LOG)

let ( >>>= ) = Lwt_result.bind

module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Batch = struct
  exception Closed

  type t =
    { mutable unwritten: int * Lwt_unix.IO_vectors.t
    ; fd: Lwt_unix.file_descr
    ; mutable async_dispatched: bool
    ; mutable latest_promise: (unit, exn) Lwt_result.t
    ; limit: int
    ; mutable closed: bool }

  let write_vec fd v expected_size =
    let rec loop remaining =
      Lwt_unix.writev fd v
      >>= fun written ->
      if written <> remaining then (
        Lwt_unix.IO_vectors.drop v written ;
        loop (remaining - written) )
      else Lwt.return_ok ()
    in
    Lwt.catch (fun () -> loop expected_size) Lwt.return_error

  let do_batch t =
    let batch = t.unwritten in
    t.unwritten <- (0, Lwt_unix.IO_vectors.create ()) ;
    t.latest_promise <-
      (t.latest_promise >>>= fun () -> write_vec t.fd (snd batch) (fst batch)) ;
    t.latest_promise

  let do_async t () =
    t.async_dispatched <- false ;
    do_batch t |> Lwt_result.get_exn

  let add buf off len t =
    if t.closed then raise Closed
    else
      let prev, vec = t.unwritten in
      Lwt_unix.IO_vectors.append_bytes vec buf off len ;
      t.unwritten <- (prev + len, vec) ;
      if fst t.unwritten > t.limit then
        Lwt.on_success (do_batch t) (function
          | Ok () ->
              Log.debug (fun m -> m "Written batch")
          | Error exn ->
              Log.debug (fun m -> m "Failed to write batch with %a" Fmt.exn exn))
      else if not t.async_dispatched then Lwt.async (do_async t)

  let sync t = do_batch t >>>= fun () -> Lwt_unix.fsync t.fd >>= Lwt.return_ok

  let close t =
    t.closed <- true ;
    sync t
    >>>= fun () ->
    Lwt.catch (fun () -> Lwt_unix.close t.fd >>= Lwt.return_ok) Lwt.return_error

  let create fd limit =
    let unwritten = (0, Lwt_unix.IO_vectors.create ()) in
    let latest_promise = Lwt.return_ok () in
    { unwritten
    ; fd
    ; async_dispatched= false
    ; latest_promise
    ; limit
    ; closed= false }
end

type batch = Batch.t

module Persistant (P : Persistable) = struct
  include P

  type t = {t: P.t; batch: batch}

  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let len = p_len + 8 in
    let buf = Bytes.create len in
    p_blit buf ~offset:8 ;
    EndianBytes.LittleEndian.set_int64 buf 0 (Int64.of_int p_len) ;
    Batch.add buf 0 len t.batch ;
    t

  let sync t =
    Batch.sync t.batch
    >>= function
    | Error exn ->
        Log.err (fun m -> m "Got error from write_promise %a" Fmt.exn exn) ;
        Lwt.return_error exn
    | Ok () ->
        (* All pending writes now complete *)
        Logs.debug (fun m -> m "Finished syncing") ;
        Lwt.return_ok ()

  let read_value channel =
    let rd_buf = Bytes.create 8 in
    Lwt_io.read_into_exactly channel rd_buf 0 8
    >>= fun () ->
    let size = EndianBytes.LittleEndian.get_int64 rd_buf 0 |> Int64.to_int in
    let payload_buf = Bytes.create size in
    Lwt_io.read_into_exactly channel payload_buf 0 size
    >>= fun () -> payload_buf |> Lwt.return

  let of_file file =
    Logs.debug (fun m -> m "Trying to open file") ;
    Lwt_unix.openfile file Lwt_unix.[O_RDONLY; O_CREAT] 0o640
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
    Lwt_stream.fold (fun v t -> P.apply t v) stream (P.init ())
    >>= fun t ->
    Lwt_io.close input_channel
    >>= fun () ->
    Logs.debug (fun m -> m "Creating fd for persistance") ;
    Lwt_unix.openfile file Lwt_unix.[O_WRONLY; O_APPEND] 0o640
    >>= fun fd ->
    let batch = Batch.create fd 128 in
    Lwt.return {t; batch}

  let change op t = {(write t op) with t= P.apply t.t op}

  let close t =
    Batch.close t.batch
    >>= function
    | Error exn ->
        Log.err (fun m -> m "Error on closing batch %a" Fmt.exn exn) ;
        Lwt.return_unit
    | Ok () ->
        Log.info (fun m -> m "Closed wal successfully") ;
        Lwt.return_unit
end
