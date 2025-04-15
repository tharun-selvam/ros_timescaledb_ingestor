# Use the official ROS Noetic base image
FROM ros:noetic-ros-base

# Set shell for subsequent commands
SHELL ["/bin/bash", "-c"]

# Prevent interactive prompts during apt-get installs
ENV DEBIAN_FRONTEND=noninteractive

# Install common tools and Python 3 essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    vim \
    python3-pip \
    python3-rosdep \
    python3-catkin-tools \
    # Add any other specific dependencies your project needs here
    # Example: sudo apt-get install ros-noetic-PACKAGE_NAME
    && rm -rf /var/lib/apt/lists/*

# Initialize rosdep
RUN apt-get update && rosdep update

# Set up the workspace directory
WORKDIR /root/ros1_ws

# Optional: Add ROS sourcing to bashrc for convenience
RUN echo "source /opt/ros/noetic/setup.bash" >> ~/.bashrc && \
    echo "source /root/ros1_ws/devel/setup.bash" >> ~/.bashrc

# Set default command to keep container running (useful for docker compose up -d)
# Change to CMD ["bash"] if you prefer the container to exit when the shell does
CMD ["tail", "-f", "/dev/null"]