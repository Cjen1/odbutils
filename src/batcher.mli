(* [src] is the logging source for the batcher *)
val src : Logs.Src.t

(* ['a t] the type of a batcher *)
type 'a t

(* [create limit f] is a batched dispatcher which
 * after [limit] calls to [auto_dispatch] will run
 * [f]
 * *)
val create : int -> 'a t

(* batched dispatchers *)
val auto_dispatch_varying : 'a t -> 'a -> int -> ('a list -> unit Lwt.t) -> unit Lwt.t
val auto_dispatch : 'a t -> 'a -> ('a list -> unit Lwt.t) -> unit Lwt.t

(* Manually dispatch *)
val perform : 'a t -> ('a list -> unit Lwt.t) -> unit Lwt.t
