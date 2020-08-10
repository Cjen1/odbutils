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

module S = Serialiser
module LU = Lwt_unix
module LE = EndianBytes.LittleEndian
             
module File = struct
  type t = 
    { fd : LU.file_descr
    ; path : string
    ; mutable length : int
    ; mutable cursor : int
    ; mutable serial : unit Serialiser.t
    ; mutable sync_cursor : int
    ; mutable state : [`Open |`Closed]
    }

  let create_empty path size =
    LU.openfile path [O_WRONLY; O_CREAT; O_TRUNC] 0o666 >>= fun fd ->
    let cursor = 0 in
    LU.ftruncate fd size >>= fun () ->
    LU.fsync fd >>= fun () ->
    Lwt.return {fd; path; cursor; length = size;serial = S.create (); sync_cursor=cursor; state=`Open}

  let create_partial path size =
    LU.openfile path [O_WRONLY; O_CREAT] 0o666 >>= fun fd ->
    LU.lseek fd 0 LU.SEEK_END >>= function
    | cursor when cursor > size ->
      Lwt.fail_invalid_arg (Fmt.str "create_partial: cursor (%d) > size (%d)" cursor size)  
    | cursor ->
      LU.ftruncate fd size >>= fun () ->
      LU.fsync fd >>= fun () ->
      Lwt.return {fd; path; cursor; length = size; serial = S.create (); sync_cursor=cursor; state=`Open}

  exception Closed
  exception OOM

  let available f =
    f.length - f.cursor

  let rec loop t buf offset size =
    Lwt_unix.write t.fd buf offset size
    >>= fun written ->
    if written = 0 then
      Lwt.fail Closed
    else if written = size then Lwt.return_unit
    else loop t buf (offset + written) (size - written)

  let schedule_write file buf off len =
    match file with
    | {state=`Closed;_} -> Lwt.fail Closed
    | _ when file.cursor + len > file.length ->
      Lwt.fail OOM
    | _ -> 
      file.cursor <- file.cursor + len; 
      let f () = loop file buf off len in
      Serialiser.serialise file.serial f >>= fun () ->
      Lwt.return_unit

  let datasync file =
    file.sync_cursor <- file.cursor;
    Serialiser.serialise file.serial (fun () ->
        LU.fdatasync file.fd 
      )

  let close file =
    file.state <- `Closed;
    Serialiser.serialise file.serial (fun () ->
        if file.cursor = 0 
        then
          LU.close file.fd >>= fun () ->
          LU.unlink file.path
        else 
          LU.ftruncate file.fd file.cursor >>= fun () ->
          LU.close file.fd
      )

  let resize file size =
    match file with
    | {cursor;_} when cursor > size ->  
      Lwt.fail_invalid_arg "Cursor greater than size"
    | _ ->
      LU.ftruncate file.fd size >>= fun () ->
      LU.fsync file.fd >>= fun () ->
      file.length <- size;
      Lwt.return_unit

  (* Write to file then close it *)
  let write_oversized t buf off len =
    let new_size = t.cursor + len in 
    Serialiser.serialise t.serial (fun () -> resize t new_size) |> ignore;
    schedule_write t buf off len
end 

module F = File

module Persistant (P: Persistable) = struct
  type write = int * (bytes -> offset:int -> unit)
  type t =
    { mutable current_file : File.t
    ; mutable next_file : File.t Lwt.t
    ; mutable syncable_promise : unit Lwt.t
    ; default_file_size : int
    ; next_name_stream : string Lwt_stream.t
    ; serial : unit Serialiser.t
    ; batcher : write Batcher.t
    }

  let collate_batch bs =
    let total_size =
      List.fold_left (fun acc (p_len, _) -> acc + p_len + 4) 0 bs
    in 
    let buf = Bytes.create total_size in
    let blitted =
      List.fold_left
        (fun offset (p_len, p_blit) ->
           LE.set_int32 buf offset (Int32.of_int p_len);
           p_blit buf ~offset:(offset + 4);
           offset + p_len + 4)
        0 bs
    in
    assert (blitted = total_size);
    buf

  let write_batch_current_file t bs = 
    let buf = collate_batch bs in
    File.schedule_write t.current_file buf 0 (Bytes.length buf) 

  let move_to_next_file t =
    let close_p = F.close t.current_file in
    t.next_file >>= fun next_file ->
    t.current_file <- next_file;
    Lwt_stream.get t.next_name_stream >|= Option.get >>= fun next_name ->
    let next_file_p = F.create_empty next_name t.default_file_size in
    t.next_file <- next_file_p;
    t.syncable_promise <- Lwt.join [close_p; t.syncable_promise];
    Lwt.return_unit

  let rec write_batch t = function 
    | [] -> 
      Log.debug (fun m -> m "Empty batch");
      Lwt.return_unit
    | (((p_len, _) as b) :: _) as bs  when p_len > F.available t.current_file && p_len > t.default_file_size ->
      Log.debug (fun m -> m "Too large of a file to be written");
      let buf = collate_batch [b] in
      F.write_oversized t.current_file buf 0 (Bytes.length buf) |> ignore;
      move_to_next_file t >>= fun () ->
      write_batch t bs
    | bs ->
      let current_file_writes, next_file_writes = 
        let (_:int), current_file, next_file = 
          List.fold_left (fun (bytes_left, cf, nf) ((p_len, _) as b) ->
              if p_len < bytes_left 
              then (bytes_left - p_len, b :: cf, nf)
              else (0, cf, b::nf)
            ) (F.available t.current_file, [],[]) bs
        in 
        List.rev current_file, List.rev next_file 
      in
      let current_write = 
        let buf = collate_batch current_file_writes in
        F.schedule_write t.current_file buf 0 (Bytes.length buf)
      in
      match next_file_writes with
      | [] -> (* Able to write entirety to single file *)
        current_write
      | bs ->
        move_to_next_file t >>= fun () ->
        write_batch t bs

  let write_batch_serial t bs =
    S.serialise t.serial (fun () -> write_batch t bs)

  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let p = Batcher.auto_dispatch t.batcher (p_len, p_blit) (write_batch_serial t) in
    Lwt.on_failure p !Lwt.async_exception_hook ;
    t

  let datasync t =
    Serialiser.serialise t.serial (fun () ->
        Lwt.join [F.datasync t.current_file ; t.syncable_promise]
      )

  let make_name_generator path =
    let count_r = ref 0 in
    fun () -> 
      let count = !count_r in
      incr count_r;
      Fmt.str "%s/%d.wal" path count
    
  let read_value channel =
    let rd_buf = Bytes.create 4 in
    Lwt_io.read_into_exactly channel rd_buf 0 4
    >>= fun () ->
    let size = LE.get_int32 rd_buf 0 |> Int32.to_int in
    if size <= 0 then
      Lwt.return_none
    else ( 
      Log.debug (fun m -> m "Reading payload of %d bytes" size) ;
      let payload_buf = Bytes.create size in
      Lwt_io.read_into_exactly channel payload_buf 0 size
      >>= fun () -> payload_buf |> Lwt.return_some
    )

  let of_dir ?(default_file_size = 1024*1024) ?(batch_size=512) path =
    let stream_generator path =
      Lwt_unix.openfile path Lwt_unix.[O_RDONLY; O_CREAT] 0o640
      >>= fun fd ->
      let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input fd in
      let stream =
        Lwt_stream.from (fun () ->
            Lwt.catch
              (fun () -> read_value input_channel)
              (function _ -> Lwt.return_none))
      in
      (Lwt_stream.closed stream >>= fun () -> Lwt_io.close input_channel) |> Lwt.ignore_result;
      Lwt.return stream
    in 
    let next_name_stream = 
      let name_generator = make_name_generator path in
      Lwt_stream.from_direct (fun () -> Some (name_generator())) 
    in 
    Lwt_stream.get_while Sys.file_exists next_name_stream >>= fun existing_names ->
    let name_stream = Lwt_stream.of_list existing_names in
    let streams = Lwt_stream.map_s stream_generator name_stream in
    let token_stream = Lwt_stream.concat streams |> Lwt_stream.map (P.decode ~offset:0) in
    Lwt_stream.fold
      (fun v t -> P.apply t v)
      token_stream (P.init ())
    >>= fun t ->
    Logs.debug (fun m -> m "Setting up files");
    Lwt_stream.get next_name_stream >|= Option.get >>= fun next_name ->
    F.create_empty next_name default_file_size >>= fun current_file ->
    Lwt_stream.get next_name_stream >|= Option.get >>= fun next_name ->
    let next_file = F.create_empty next_name default_file_size in 
    ({ current_file
    ; next_file
    ; syncable_promise=Lwt.return_unit
    ; default_file_size
    ; next_name_stream
    ; serial = Serialiser.create ()
    ; batcher = Batcher.create batch_size
    }, t) |> Lwt.return 

  let close t =
    F.close t.current_file >>= fun () ->
    t.next_file >>= fun next_file ->
    F.close next_file
end 
