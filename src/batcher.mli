(* [src] is the logging source for the batcher *)
val src : Logs.Src.t

(* ['a t] the type of a batcher *)
type 'a t

(* [create limit f] is a batched dispatcher which
 * after [limit] calls to [auto_dispatch] will run
 * [f]
 * *)
val create : int -> ('a list -> unit Lwt.t) -> 'a t

(* batched dispatcher*)
val auto_dispatch : 'a t -> 'a -> unit Lwt.t

(* Manually dispatch *)
val perform : 'a t -> unit Lwt.t
