open Lwt.Infix

let src = Logs.Src.create "Batcher" ~doc:"Batcher"

module Log = (val Logs.src_log src : Logs.LOG)

module type Countable = sig
  type t
  val compare : t -> t -> int
  val incr : t -> t
  val init : unit -> t
  val init_limit : t -> t
end 

module type Batcher = sig
  (* ['a t] the type of a batcher *)
  type 'a t

  val auto_dispatch : 'a t ->  ('a list -> unit Lwt.t) -> 'a -> unit Lwt.t 

  (* Manually dispatch *)
  val perform : 'a t -> ('a list -> unit Lwt.t) -> unit Lwt.t
end 

(* Comparison between a counter and current *)
module Make (C: Countable) = struct
  type 'a t =
    { limit: C.t
    ; mutable batch_limit : C.t option
    ; mutable current: C.t
    ; mutable dispatched: bool
    ; mutable outstanding: 'a list }

  let perform t f =
    let vs = t.outstanding |> List.rev in
    t.outstanding <- [];
    t.current <- C.init ();
    t.batch_limit <- Some (C.init_limit t.limit);
    t.dispatched <- false ;
    f vs

  let rec auto_dispatch t f v =
    match t.batch_limit with
    | None ->
      t.batch_limit <- Some (C.init_limit t.limit);
      auto_dispatch t f v
    | Some batch_limit ->
      match v with
      | v when C.compare (C.incr t.current) batch_limit >= 0 ->
        Log.debug (fun m -> m "batch full, handling") ;
        t.outstanding <- v :: t.outstanding ;
        t.current <- C.incr t.current;
        perform t f
      | v when not t.dispatched ->
        Log.debug (fun m -> m "dispatching async handler") ;
        t.dispatched <- true ;
        t.outstanding <- v :: t.outstanding ;
        t.current <- C.incr t.current;
        Lwt.async (fun () ->
            Lwt.pause ()
            (* Required since Lwt runs this up to the first promise *)
            >>= fun () -> perform t f);
        Lwt.return_unit
      | v ->
        Log.debug (fun m -> m "adding element") ;
        t.outstanding <- v :: t.outstanding ;
        t.current <- C.incr t.current;
        Lwt.return_unit
end 

module Count = struct
  module S = Make (struct
    type t = int
    let compare = Int.compare
    let incr i = i + 1
    let init () = 0
    let init_limit l = l
  end )
  include S
  let create limit =
    S.{limit; batch_limit = None; dispatched = false; outstanding = []; current = 0}
end 

module Time = struct
  module S = Make (struct
    type t = float
    let compare = Float.compare
    let incr _ = Unix.gettimeofday ()
    let init () = Unix.gettimeofday ()
    let init_limit limit = Unix.gettimeofday () +. limit 
  end )
  include S
  let create limit =
    S.{limit; batch_limit = None; dispatched = false; outstanding = []; current = Unix.gettimeofday ()}
end 
