(lang dune 2.5)
(name odbutils)
(source (github cjen1/owal))
(authors "Chris Jensen")

(generate_opam_files true)

(package
  (name odbutils)
  (synopsis "Set of utilities for building database systems")
  (depends
    lwt
    logs
    fmt
    base
    ))

