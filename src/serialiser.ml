open Lwt.Infix

type 'a t = {mutable promise: 'a Lwt.t; mutable length: int}

let serialise ser f =
  ser.length <- ser.length + 1 ;
  let p =
    ser.promise
    >>= fun v ->
    f v
    >>= fun v ->
    ser.length <- ser.length - 1 ;
    Lwt.return v
  in
  ser.promise <- p ;
  p

let create () = {promise= Lwt.return_unit; length= 0}
