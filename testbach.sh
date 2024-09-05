rm build -r
mkdir build && cd build
autoreconf -iv ..
../configure --enable-optimize --disable-debug --with-bach=/home/caoyihao/bach/BACH_demo/build/
make check -j
