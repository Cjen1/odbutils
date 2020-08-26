(* [src] is the logging source for the batcher *)
val src : Logs.Src.t

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

  val auto_dispatch : 'a t -> ('a list -> unit Lwt.t ) -> 'a -> unit Lwt.t 

  (* Manually dispatch *)
  val perform : 'a t -> ('a list -> unit Lwt.t) -> unit Lwt.t
end 

module Make (C : Countable) : Batcher

(* Number of items based batching *)
module Count : sig
  include Batcher
  (* [create limit] Creates a batcher that batches every [limit] requests *)
  val create : int -> 'a t
end 

(* Time since last entry based batching*)
module Time : sig
  include Batcher
  (* [create limit] Creates a batcher that batches every [limit] seconds *)
  val create : float -> 'a t
end 

