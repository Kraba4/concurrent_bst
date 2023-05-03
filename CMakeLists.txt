cmake_minimum_required(VERSION 3.23)
project(bachelor)

set(CMAKE_CXX_STANDARD 20)

add_executable(bachelor main.cpp ConcurrentPartiallyExternalTree.h)
