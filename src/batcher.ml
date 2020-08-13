open Lwt.Infix

let src = Logs.Src.create "Batcher" ~doc:"Batcher"

module Log = (val Logs.src_log src : Logs.LOG)

type 'a t =
  { batch_limit: int
  ; mutable count : int
  ; mutable dispatched: bool
  ; mutable outstanding: 'a list }

let perform t f =
  let vs = t.outstanding |> List.rev in
  t.outstanding <- [] ;
  t.count <- 0;
  t.dispatched <- false ;
  f vs

let auto_dispatch_varying t v l f = match v with
  | v when t.count + l >= t.batch_limit ->
    Log.debug (fun m -> m "batch full, handling");
    t.outstanding <- v :: t.outstanding;
    t.count <- t.count + 1;
    perform t f
  | v when not t.dispatched ->
    Log.debug (fun m -> m "dispatching async handler");
    t.dispatched <- true;
    t.outstanding <- v :: t.outstanding;
    t.count <- t.count + 1;
    Lwt.async (fun () ->
        Lwt.pause ()
        (* Required since Lwt runs this up to the first promise *)
        >>= fun () -> perform t f) ;
    Lwt.return_unit
  | v ->
    Log.debug (fun m -> m "adding element");
    t.outstanding <- v :: t.outstanding ;
    t.count <- t.count + 1;
    Lwt.return_unit

let auto_dispatch t v f = auto_dispatch_varying t v 1 f

let create batch_limit = {batch_limit; dispatched= false; outstanding= []; count=0}
