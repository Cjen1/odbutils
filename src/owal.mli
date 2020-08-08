module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t

  type op = P.op

  val sync : t -> (unit, exn) Lwt_result.t

  val of_file : string -> t Lwt.t

  val change : op -> t -> t

  val close : t -> unit Lwt.t

  val do_one_cycle : t -> unit Lwt.t 

  val get_underlying : t -> P.t
end
