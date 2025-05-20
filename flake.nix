{
  description = "Rust devShell with Fenix stable toolchain";

  nixConfig = {
    extra-substituters = [ "https://nix-community.cachix.org" ];
    extra-trusted-public-keys = [
      "nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs"
    ];
  };

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rustowl-flake.url = "github:nix-community/rustowl-flake";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-utils,
      fenix,
      rustowl-flake,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        rustToolChain = fenix.packages.${system}.stable.toolchain;
      in
      {
        formatter = pkgs.nixfmt-rfc-style;

        devShells.default =
          with pkgs;
          mkShell {
            buildInputs = [
              rustowl-flake.packages.${system}.rustowl
              lldb_20 # use lldb-dap for debug rust with vscode(Codelldb is buggy)
            ];
            packages = [ rustToolChain ];
          };
      }
    );
}
