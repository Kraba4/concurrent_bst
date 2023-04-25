cmake_minimum_required(VERSION 3.23)
project(bachelor)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g ")
add_executable(bachelor main.cpp ConcurrentPartiallyExternalTree.h)
