open Lwt.Infix

let src = Logs.Src.create "Batcher" ~doc:"Batcher"

module Log = (val Logs.src_log src : Logs.LOG)

type 'a t =
  { batch_limit: int
  ; mutable dispatched: bool
  ; mutable outstanding: 'a list }

let perform t f =
  let vs = t.outstanding |> List.rev in
  t.outstanding <- [] ;
  t.dispatched <- false ;
  f vs

let auto_dispatch t v f = match v with
  | v when List.length t.outstanding + 1 >= t.batch_limit ->
      Log.debug (fun m -> m "Batcher: batch full, handling") ;
      t.outstanding <- v :: t.outstanding ;
      perform t f
  | v when not t.dispatched ->
      Log.debug (fun m -> m "Batcher: dispatching async handler") ;
      t.dispatched <- true ;
      t.outstanding <- v :: t.outstanding ;
      Lwt.async (fun () ->
          Lwt.pause ()
          (* Required since Lwt runs this up to the first promise *)
          >>= fun () -> perform t f) ;
      Lwt.return_unit
  | v ->
      Log.debug (fun m -> m "Batcher: adding") ;
      t.outstanding <- v :: t.outstanding ;
      Lwt.return_unit

let create batch_limit = {batch_limit; dispatched= false; outstanding= []}
