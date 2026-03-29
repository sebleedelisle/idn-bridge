cmake_minimum_required(VERSION 3.18)

project(IDNBridge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(IDN_BRIDGE_USE_BUNDLED_LIBUSB "Use bundled libusb from libera-core dependency tree" ON)
option(IDN_BRIDGE_OPENIDN_BRIDGE_MODE "Enable bridge-specific OpenIDN behavior changes" ON)

set(LIBERA_USE_BUNDLED_LIBUSB ${IDN_BRIDGE_USE_BUNDLED_LIBUSB} CACHE BOOL "" FORCE)
set(LIBERA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LIBERA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LIBERA_BUILD_INTEGRATION_TESTS OFF CACHE BOOL "" FORCE)
set(LIBERA_BUILD_HARDWARE_TESTS OFF CACHE BOOL "" FORCE)

add_subdirectory(extern/libera-core)

set(OPENIDN_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/extern/helios_openidn/server-src/helios_openidn")

set(OPENIDN_SOURCES
    "${OPENIDN_ROOT}/shared/AdapterBase.cpp"
    "${OPENIDN_ROOT}/shared/DACHWInterface.cpp"
    "${OPENIDN_ROOT}/shared/DecoderBase.cpp"
    "${OPENIDN_ROOT}/shared/LaproAdapter.cpp"
    "${OPENIDN_ROOT}/shared/ODFTools.cpp"
    "${OPENIDN_ROOT}/output/RTOutput.cpp"
    "${OPENIDN_ROOT}/output/RTLaproGraphOut.cpp"
    "${OPENIDN_ROOT}/output/IDNLaproDecoder.cpp"
    "${OPENIDN_ROOT}/output/IdtfDecoder.cpp"
    "${OPENIDN_ROOT}/output/STDLaproGraphOut.cpp"
    "${OPENIDN_ROOT}/output/V1LaproGraphOut.cpp"
    "${OPENIDN_ROOT}/server/IDNService.cpp"
    "${OPENIDN_ROOT}/server/IDNSession.cpp"
    "${OPENIDN_ROOT}/server/IDNLaproService.cpp"
    "${OPENIDN_ROOT}/server/IDNLaproGraConInlet.cpp"
    "${OPENIDN_ROOT}/server/IDNLaproGraDisInlet.cpp"
    "${OPENIDN_ROOT}/server/IDNServer.cpp"
    "${OPENIDN_ROOT}/stage/SockIDNServer.cpp"
)

if(NOT IDN_BRIDGE_OPENIDN_BRIDGE_MODE)
  list(APPEND OPENIDN_SOURCES
      "${OPENIDN_ROOT}/hardware/Helios/HeliosAdapter.cpp"
  )
endif()

add_library(openidn-core STATIC ${OPENIDN_SOURCES})
target_compile_definitions(openidn-core PUBLIC ODF_USE_TAXI_SOCK)
if(IDN_BRIDGE_OPENIDN_BRIDGE_MODE)
  target_compile_definitions(openidn-core PUBLIC OPENIDN_BRIDGE_MODE=1)
endif()
target_include_directories(openidn-core PUBLIC "${OPENIDN_ROOT}")
if(MSVC)
  target_compile_options(openidn-core PRIVATE /W0)
else()
  target_compile_options(openidn-core PRIVATE -w)
endif()

find_package(Threads REQUIRED)
target_link_libraries(openidn-core PUBLIC Threads::Threads)

add_executable(idn_bridge src/main.cpp)
target_include_directories(idn_bridge PRIVATE "${OPENIDN_ROOT}")
target_link_libraries(idn_bridge PRIVATE libera-core openidn-core Threads::Threads)
