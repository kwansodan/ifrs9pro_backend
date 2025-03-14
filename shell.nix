{ pkgs ? import (fetchTarball "https://nixos.org/channels/nixos-24.05/nixexprs.tar.xz") {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.poetry
    pkgs.python312
    pkgs.act
  ];

  LD_LIBRARY_PATH="${pkgs.libGL}/lib/:${pkgs.stdenv.cc.cc.lib}/lib/:${pkgs.glib.out}/lib/";

}
