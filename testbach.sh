rm build -r
mkdir build && cd build
autoreconf -iv ..
../configure --enable-optimize --enable-debug --with-bach=/mnt/d/BACH_demo/build/
make check -j
