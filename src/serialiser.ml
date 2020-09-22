type 'a t = {push: (unit -> unit Lwt.t) -> unit Lwt.t}

let serialise ser f = ser.push f

let create () =
  let stream, push = Lwt_stream.create () in
  let push e =
    let v, f = Lwt.task () in
    push (Some (f, e)) ;
    v
  in
  let fold (ful, f) () =
    let p = Lwt.apply f () in
    Lwt.on_any p
      (* Success *)
        (fun () -> Lwt.wakeup ful ())
      (* Failure *)
        (fun exn -> Lwt.wakeup_exn ful exn) ;
    p
  in
  let serialiser_thread = Lwt_stream.fold_s fold stream () in
  Lwt.on_failure serialiser_thread (fun exn ->
      Logs.debug (fun m ->
          m "Encountered error while serialising: %a" Fmt.exn_backtrace
            (exn, Printexc.get_raw_backtrace ()))) ;
  {push}
