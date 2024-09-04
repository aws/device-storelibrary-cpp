# device-storelibrary-cpp

The device-storelibrary-cpp Stream Store and KV Store encapsulate best practices for reliable data persistence including handling all types of data corruption that happens to edge devices.
The database also provides simple primitives enabling reliable data upload by offering persistent iterators to track what is successfully uploaded or not.

# Getting Started
## Requirements
1. Install CMake and the relevant build tools for your platform. Ensure these are available in your executable path.

## Using the Device Store Library
Example usage of Stream Store and KV Store can be found [here](https://github.com/aws/device-storelibrary-cpp/blob/main/src/main.cpp#L48)

## Building the Device Store Library
### Building from source
1. Clone this repository `git clone https://github.com/aws/device-storelibrary-cpp.git`
2. Create your build directory. Replace `<BUILD_DIR>` with your build directory name 
3. Build the project: 
    ```
    cd <BUILD_DIR>
    cmake <path-to-root-of-this-source-code> \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=<path-to-install> \
    cmake --build . --config=Release
    cmake --install . --config=Release
    ```
   
### Consume Library using CPM
1. Add the following single line to your `CMakeLists.txt` which points to the library on GitHub along with a commit ID, tag, or branch name ([More CPM information can be found here](https://github.com/cpm-cmake/CPM.cmake)). 
    ```
    CPMAddPackage(
      NAME device-storelibrary-cpp
      VERSION 1.0.0
      GITHUB_REPOSITORY aws/device-storelibrary-cpp
    )
    ```

### Consume Library as a Git Submodule
1. Add this repository as a submodule in your project `git submodule add https://github.com/aws/device-storelibrary-cpp.git`

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

