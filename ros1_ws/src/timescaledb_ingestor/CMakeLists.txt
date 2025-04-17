cmake_minimum_required(VERSION 3.0.2)
project(timescaledb_ingestor)

# Find required ROS packages
find_package(catkin REQUIRED COMPONENTS
  roscpp
  sensor_msgs
)

# Find libpqxx using pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(Libpqxx REQUIRED IMPORTED_TARGET libpqxx)

# --- Add explicit checks and messages ---
if(Libpqxx_FOUND)
  message(STATUS "Found libpqxx via pkg-config.")
  message(STATUS "  Includes: ${Libpqxx_INCLUDE_DIRS}")
  message(STATUS "  Libraries: ${Libpqxx_LIBRARIES}") # Just checking the library name
else()
  message(WARNING "Could NOT find libpqxx via pkg-config. Attempting manual find.")
  # Fallback: Try find_path (less robust)
  find_path(Manual_Libpqxx_INCLUDE_DIR NAMES libpqxx/libpqxx
            PATHS /usr/include /usr/local/include /opt/include) # Add common paths
  if(Manual_Libpqxx_INCLUDE_DIR)
      message(STATUS "Found libpqxx headers manually at: ${Manual_Libpqxx_INCLUDE_DIR}")
      set(Libpqxx_INCLUDE_DIRS ${Manual_Libpqxx_INCLUDE_DIR}) # Manually set the variable
  else()
      message(FATAL_ERROR "Could not find libpqxx headers anywhere. Please install libpqxx-dev correctly.")
  endif()
endif()

# Find nlohmann_json package
find_package(nlohmann_json 3.2.0) # Check version if necessary

# Add C++ standard requirement
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- Add Catkin Includes ---
include_directories(
  ${catkin_INCLUDE_DIRS}
  ${Libpqxx_INCLUDE_DIRS}
  ${nlohmann_json_INCLUDE_DIRS}
)
# --- End Add Catkin Includes ---

catkin_package(
#  INCLUDE_DIRS include
#  LIBRARIES timescaledb_ingestor
  CATKIN_DEPENDS roscpp sensor_msgs
#  DEPENDS libpqxx
)

# Add executable
add_executable(timescaledb_ingestor_cpp_node src/timescaledb_ingestor_node.cpp)

# Link libraries
target_link_libraries(timescaledb_ingestor_cpp_node
  ${catkin_LIBRARIES} # <-- Ensure this is present
  PkgConfig::Libpqxx
  nlohmann_json::nlohmann_json
  Threads::Threads
)

# Optional: Add install rules...