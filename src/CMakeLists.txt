# set source files
file (GLOB src-files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (AAA-pkg "share/cmake/AAA")

# set version fo shared library
set (AAA-vers "${BBB_VERSION_MAJOR}.${BBB_VERSION_MINOR}")

# target library
add_library (BBB ${src-files})
target_link_libraries (BBB
    PUBLIC diaspora-stream-api
    PRIVATE coverage_config warnings_config)
target_include_directories (BBB PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (BBB BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (BBB BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (BBB
    PROPERTIES VERSION ${DDD_VERSION}
    SOVERSION ${DDD_VERSION_MAJOR})

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "AAA-config-version.cmake"
    VERSION ${DDD_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (AAA-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/AAA-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mofka/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

install (TARGETS BBB
         EXPORT AAA-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT AAA-targets
         DESTINATION ${AAA-pkg}
         FILE "AAA-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/AAA-config.cmake"
         "${CMAKE_CURRENT_BINARY_DIR}/AAA-config-version.cmake"
         DESTINATION ${AAA-pkg} )
install (DIRECTORY ../include/AAA
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
