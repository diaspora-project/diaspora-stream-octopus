# set source files
file (GLOB src-files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (octopus-pkg "share/cmake/octopus")

# set version fo shared library
set (octopus-vers "${octopus_VERSION_MAJOR}.${octopus_VERSION_MINOR}")

# target library
add_library (octopus ${src-files})
target_link_libraries (octopus
    PUBLIC diaspora-stream-api
    PRIVATE coverage_config warnings_config)
target_include_directories (octopus PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (octopus BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (octopus BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (octopus
    PROPERTIES VERSION ${OCTOPUS_VERSION}
    SOVERSION ${OCTOPUS_VERSION_MAJOR})

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "octopus-config-version.cmake"
    VERSION ${OCTOPUS_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (octopus-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/octopus-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mofka/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

install (TARGETS octopus
         EXPORT octopus-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT octopus-targets
         DESTINATION ${octopus-pkg}
         FILE "octopus-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/octopus-config.cmake"
         "${CMAKE_CURRENT_BINARY_DIR}/octopus-config-version.cmake"
         DESTINATION ${octopus-pkg} )
install (DIRECTORY ../include/octopus
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
