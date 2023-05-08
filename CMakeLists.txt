cmake_minimum_required(VERSION 3.23)
project(bachelor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(bachelor main.cpp ConcurrentPartiallyExternalTree.h Old.h)
