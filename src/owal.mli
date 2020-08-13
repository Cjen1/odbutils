
module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Persistant(P: Persistable) : sig
  type t

  val of_dir : ?default_file_size:int -> ?batch_size:int -> string -> (t * P.t) Lwt.t

  val datasync : t -> unit Lwt.t
  val write : t -> P.op -> unit Lwt.t
  val close : t -> unit Lwt.t 
end
