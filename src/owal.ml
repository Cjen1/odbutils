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

module File = struct
  type t =
    { path: string
    ; writer: Async.Writer.t
    ; mutable cursor: int64
    ; mutable length: int64
    ; mutable state: [`Closed | `Open] }

  let create path ~len ~append =
    let%bind writer = Writer.open_file ~append path in
    let%bind () = Unix.ftruncate (Writer.fd writer) ~len in
    let%bind () = Writer.fsync writer in
    return {path; writer; cursor= Int64.zero; length= len; state= `Open}

  let close t =
    match Deferred.is_determined @@ Writer.close_started t.writer with
    | true ->
        (*print_endline @@ Fmt.str "%s: file.state closed" t.path;*)
        Writer.close_finished t.writer
    | false ->
        (*print_endline @@ Fmt.str "%s: file.state open" t.path;*)
        if Int64.(t.cursor = zero) then
          (*print_endline @@ Fmt.str "%s: empty file" t.path;*)
          let%bind () = Writer.close t.writer in
          Unix.unlink t.path
        else
          (*print_endline @@ Fmt.str "%s: truncating" t.path;*)
          let%bind () = Unix.ftruncate (Writer.fd t.writer) ~len:t.cursor in
          Writer.close t.writer

  let datasync t = Writer.fdatasync t.writer

  let write_bin_prot t (bin_prot : 'a Core.Bin_prot.Type_class.writer) v =
    let len =
      bin_prot.size v + Bin_prot.Utils.size_header_length |> Int64.of_int
    in
    match Deferred.is_determined @@ Writer.close_started t.writer with
    | true ->
        Error `Closed
    | false when Int64.(t.cursor + len > t.length) ->
        Error `OOM
    | false ->
        t.cursor <- Int64.(t.cursor + len) ;
        Writer.write_bin_prot t.writer bin_prot v ;
        Ok ()

  let write_bin_prot_oversized t (bin_prot : 'a Core.Bin_prot.Type_class.writer)
      v =
    let len =
      bin_prot.size v + Bin_prot.Utils.size_header_length |> Int64.of_int
    in
    t.cursor <- Int64.(t.cursor + len) ;
    Writer.write_bin_prot t.writer bin_prot v
end

module Persistant (P : Persistable) = struct
  type t =
    { default_file_size: int64
    ; mutable current_file: File.t
    ; mutable next_file: File.t Deferred.t
    ; name_gen: unit -> string
    ; mutable next_file_queue: unit Deferred.t
    ; mutable next_file_ready: unit Ivar.t
    ; mutable prev_sync: unit Deferred.t }

  let move_to_next_file t =
    (*print_endline "Moving to next file";*)
    let ready = t.next_file_ready in
    t.next_file_ready <- Ivar.create () ;
    let%bind next_file = t.next_file in
    t.current_file <- next_file ;
    (* Signal ready to write to next file *)
    Ivar.fill ready () ;
    let next_name = t.name_gen () in
    t.next_file <- File.create next_name ~append:false ~len:t.default_file_size ;
    return ()

  let rec write_bin_prot t (bin_prot : 'a Core.Bin_prot.Type_class.writer) v =
    match File.write_bin_prot t.current_file bin_prot v with
    | Error `Closed ->
        let p =
          let%bind () = t.next_file_queue in
          write_bin_prot t bin_prot v |> return
        in
        t.next_file_queue <- p ;
        don't_wait_for p
    | Error `OOM ->
        (*print_endline "OOM";*)
        File.write_bin_prot_oversized t.current_file bin_prot v ;
        t.prev_sync <- Deferred.all_unit [File.close t.current_file; t.prev_sync] ;
        don't_wait_for (move_to_next_file t)
    | Ok () ->
      ()

  let datasync t = Deferred.all_unit [t.prev_sync; File.datasync t.current_file]

  let of_path path default_file_size =
    let%bind _create_dir_if_needed =
      match%bind Sys.file_exists path with
      | `Yes ->
          return ()
      | _ ->
          Unix.mkdir path
    in
    let name_gen =
      let counter = ref 0 in
      fun () ->
        incr counter ;
        Fmt.str "%s/%d.wal" path !counter
    in
    let rec read_value_loop t reader =
      match%bind Reader.read_bin_prot reader P.bin_reader_op with
      | `Eof ->
          return t
      | `Ok op ->
          read_value_loop (P.apply t op) reader
    in
    let rec read_file_loop v =
      let path = name_gen () in
      let%bind res =
        try_with (fun () -> Reader.with_file path ~f:(read_value_loop v))
      in
      match res with Ok v -> read_file_loop v | Error _ -> return (v, path)
    in
    let%bind v, path = read_file_loop (P.init ()) in
    let%bind current_file =
      File.create path ~len:default_file_size ~append:false
    in
    let next_file =
      File.create (name_gen ()) ~len:default_file_size ~append:false
    in
    return
      ( { name_gen
        ; default_file_size
        ; current_file
        ; next_file
        ; next_file_queue= return ()
        ; next_file_ready= Ivar.create ()
        ; prev_sync= return () }
      , v )

  let write t op = write_bin_prot t P.bin_writer_op op

  let close t =
    (*print_endline "closing";*)
    let curr =
      (*print_endline"curr";*)
      let%bind () = File.close t.current_file in
      (*print_endline "closed current";*)
      return ()
    in
    let next =
      (*print_endline"next";*)
      let%bind next = t.next_file in
      let%bind () = File.close next in
      (*print_endline "closed next";*)
      return ()
    in
    let prev =
      let%bind () = t.prev_sync in
      (*print_endline "closed prev";*)
      return ()
    in
    Deferred.all_unit [curr; next; prev]
end
