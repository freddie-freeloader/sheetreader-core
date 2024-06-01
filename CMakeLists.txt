cmake_minimum_required(VERSION 3.28)
project(sheetreader_core)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS -pthread)

include_directories(src)
include_directories(src/fast_double_parser)
include_directories(src/miniz)

add_executable(sheetreader_core
        src/fast_double_parser/fast_double_parser.h
        src/miniz/miniz.cpp
        src/miniz/miniz.h
        src/main.cpp
#        src/copy.cpp
        src/parsing.h
        src/XlsxFile.cpp
        src/XlsxFile.h
        src/XlsxSheet.cpp
        src/XlsxSheet.h
#        src/copy.cpp
)
