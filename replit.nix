{pkgs}: {
  deps = [
    pkgs.pkg-config
    pkgs.arrow-cpp
    pkgs.glibcLocales
    pkgs.postgresql
    pkgs.openssl
  ];
}
