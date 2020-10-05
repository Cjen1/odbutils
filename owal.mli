open! Core
open! Async

module type Persistable = sig
  type t

  val init : unit -> t

  type op [@@deriving bin_io]

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t

  val of_path :
    string -> (t * P.t) Deferred.t

  val write : t -> P.op -> unit

  val datasync : t -> unit Deferred.t

  val close : t -> unit Deferred.t
end
