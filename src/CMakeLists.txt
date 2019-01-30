cmake_minimum_required(VERSION 3.6)   # CMake version check
project(hw3)               # Create project "simple_example"
set(CMAKE_CXX_STANDARD 11)            # Enable c11 standard
SET(CMAKE_CXX_COMPILER /usr/bin/g++)
#set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} -g -Wall)
#set(CMAKE_C_STANDARD_LIBRARIES  ${CMAKE_C_STANDARD_LIBRARIES} -I/usr/include/libvirt)
#add_library(-I/usr/include/libvirt/lvirt)
set(SOURCE_FILES store.cc threadpool.h)            # Add main.cpp file of project root directory as source file
add_executable(hw3 ${SOURCE_FILES})       # Add executable target with source files listed in SOURCE_FILES variable