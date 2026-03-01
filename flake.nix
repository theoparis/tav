{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
  };

  outputs =
    { self, nixpkgs, ... }:
    let
      eachSystem = nixpkgs.lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "riscv64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
    in
    {
      packages = eachSystem (
        system: with nixpkgs.legacyPackages.${system}; {
          default = buildGo126Module {
            pname = "tav";
            version = "0.1.0";
            src = self;

            vendorHash = "sha256-X14SByLPzvN6CF4SUB7sRgMQuIh78oYr/wUOSyi9tr4=";
          };
        }
      );

      devShells = eachSystem (
        system: with nixpkgs.legacyPackages.${system}; {
          default = mkShell {
            nativeBuildInputs = [
              go_1_26
              gopls
            ];
          };
        }
      );
    };
}
