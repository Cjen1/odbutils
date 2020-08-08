open Lwt.Infix

let src = Logs.Src.create "Owal"

module Log = (val Logs.src_log src : Logs.LOG)

module Serialiser = struct
type 'a t =
  { mutable promise : 'a Lwt.t 
  ; mutable length : int }

let serialise ser f =
  ser.length <- ser.length + 1;
  let p =
    ser.promise >>= fun v -> 
    f v >>= fun v ->
    ser.length <- ser.length - 1;
    Lwt.return v
  in 
  ser.promise <- p;
  p

let create () =
  {promise = Lwt.return_unit; length = 0}
end

module S = Serialiser
module LU = Lwt_unix
             
module File = struct
  type t = 
    { fd : LU.file_descr
    ; length : int
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
    Lwt.return {fd; cursor; length = size;serial = S.create (); sync_cursor=cursor; state=`Open}

  let create_partial path size =
    LU.openfile path [O_WRONLY; O_CREAT] 0o666 >>= fun fd ->
    LU.lseek fd 0 LU.SEEK_END >>= function
    | cursor when cursor > size ->
      Lwt.fail_invalid_arg (Fmt.str "create_partial: cursor (%d) > size (%d)" cursor size)  
    | cursor ->
      LU.ftruncate fd size >>= fun () ->
      LU.fsync fd >>= fun () ->
      Lwt.return {fd; cursor; length = size; serial = S.create (); sync_cursor=cursor; state=`Open}

  exception Closed
  exception OOM

  let access file size f =
    match file with
    | {state=`Closed;_} -> Lwt.fail Closed
    | _ when file.cursor + size > file.length ->
      Lwt.fail OOM
    | _ -> 
      file.cursor <- file.cursor + size; 
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
        LU.ftruncate file.fd file.cursor >>= fun () ->
        LU.close file.fd
      )

  let resize file size =
    match file with
    | {cursor;_} when cursor > size ->  
      Lwt.fail_invalid_arg "Cursor greater than size"
    | _ ->
      file.state <- `Closed;
      LU.ftruncate file.fd size >>= fun () ->
      {file with state = `Open; length = size} |> Lwt.return
end 

module F = File

type t =
  { mutable current_file : File.t
  ; mutable next_file : File.t
  }

let rec write_batch t = function 
  | [] -> 
    Log.debug (fun m -> m "Empty batch");
    Lwt.return_unit
  | bs ->
    let total_size =
      List.fold_left (fun acc (p_len, _) -> acc + p_len + 4) 0 bs
    in 
    let buf = Bytes.create total_size in
    let blitted =
      List.fold_left
        (fun offset (p_len, p_blit) ->
           EndianBytes.LittleEndian.set_int32 buf offset (Int32.of_int p_len);
           p_blit buf ~offset:(offset + 8);
           offset + p_len + 8)
        0 bs
    in
    assert (blitted = total_size);
    let rec loop offset size =
      Lwt_unix.write t.current_file.fd buf offset size
      >>= fun written ->
      if written = 0 then
        Lwt.fail File.Closed
      else if written = size then Lwt.return_unit
      else loop (offset + written) (size - written)
    in 
    let write = File.access t.current_file total_size (fun () ->
        loop 0 total_size
      ) in
    match Lwt.state write with
    | Lwt.Fail File.Closed ->
(*TODO FINISH*)
