# Use the official ROS Noetic base image (includes C++ build tools)
FROM ros:noetic-ros-base

# Set shell for subsequent commands
SHELL ["/bin/bash", "-c"]

# Prevent interactive prompts during apt-get installs
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies for C++ node and Python utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    # C++ DB and JSON libraries
    libpqxx-dev \
    nlohmann-json3-dev \
    pkg-config \
    # Python essentials
    python3-pip \
    python3-rosdep \
    python3-catkin-tools \
    # Common utilities
    git \
    vim \
    # Add any other system dependencies your specific sensors/algorithms might need
    && rm -rf /var/lib/apt/lists/*

# Install Python DB driver (optional, but useful if mixing Python/C++)
RUN pip3 install --no-cache-dir psycopg2-binary

# Initialize rosdep (ignore error if already initialized) and update
RUN rosdep init || true && \
    rosdep update

# Set up a default workspace directory (user will mount their actual code here)
WORKDIR /root/ros1_ws

# Add ROS sourcing to bashrc for convenience when attaching interactively
# Sources both the base ROS install and the potential workspace overlay
RUN echo "source /opt/ros/noetic/setup.bash" >> ~/.bashrc && \
    echo "if [ -f /root/ros1_ws/devel/setup.bash ]; then source /root/ros1_ws/devel/setup.bash; fi" >> ~/.bashrc

# Set default command (e.g., keep container running or open bash)
CMD ["bash"]
# Or use: CMD ["tail", "-f", "/dev/null"] # To keep it running idly