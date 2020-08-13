(lang dune 2.5)
(name odbutils)
(source (github cjen1/owal))
(authors "Chris Jensen")
(maintainers "Chris Jensen (cjen1, cjj39@cam.ac.uk)")

(generate_opam_files true)

(package
  (version 0.1)
  (name odbutils)
  (synopsis "Set of utilities for building database systems")
  (depends
    lwt
    logs
    fmt
    (bos :with-test)
    (owl :with-test)
    (alcotest :with-test)
    ))

