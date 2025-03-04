{ pkgs ? import (fetchTarball "https://nixos.org/channels/nixos-24.05/nixexprs.tar.xz") {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.poetry
    pkgs.python312
    pkgs.act
  ];

}
