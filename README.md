<!--
Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.

This file is part of nepi-engine
(see https://github.com/nepi-engine).

License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
-->
# NEPI-BOT #
This repository provides the NEPI-BOT application, which serves as the edge-side facilitator of NEPI-CONNECT, allowing edge devices to connect to a NEPI-CONNECT site-local or cloud instance. 

See [NEPI-CONNECT User Manual](https://numurus.com/wp-content/uploads/Users-Manual-NEPI-Connect.pdf) for general NEPI-CONNECT details

### Organization of the repository ####
This repository is included in _nepi_engine_ws_ as a git submodule. Most users will want to refer to that master repository's README before proceeding here.

Within this repository source code is primarily organized as a collection of python source files and an accompanying set Git submodules. You can clone and initialize all at once with
```
$ git clone --recurse-submodules <this-repo-URL>
```

or in stages with
```
$ git clone <this-repo-URL>
$ cd <this-repo>
$ git submodule init
$ git submodule update
```

## Run-time Environment ##
The NEPI-BOT application runs on any suitably-prepared python-enabled system. Typically it runs on an embedded (edge) device, though in some development, test, and production applications it may be useful to run it on a Desktop system.

> At present, Numurus only tests NEPI-BOT on Ubuntu 18.04 and 20.04-based systems

## Build and Configure NEPI-BOT on Edge Device
### Deploying Source Code to the Target
Refer to the _nepi_engine_ws_ README for full nepi-engine source code deployment instructions. The rest of these instructions assume that you have deployed nepi-bot source code to
/mnt/nepi_storage/nepi_src/nepi_engine_ws/src/nepi-bot
on the target device. From there it can be built and then installed elsewhere on the target filesystem as described below. You may also prefer to use the unified _nepi_engine_build_complete.sh_ build script within _nepi_engine_ws_.

### Initial Build-system Setup ###
Before building this project, you must set up the build environment, which includes installing a few dependencies and preparing a Python virtual environment.

### Installing Target Dependencies

This repository includes an installer script, _install_dependencies.sh_, to install and configure build-time and runtime dependencies on the target device, assuming that the target device provides a bash interpretter and the aptitude package installer. You can install all dependencies, set up python virtual environment, etc. by running the script
```
$ ./install_dependencies.sh
```
For convenience, the top-level dependencies are listed here:
- socat
- protobuf-compiler
- cmake
- python 3.6+
- pip3
- virtualenv

Other python dependencies are installed in a python virtualenv (virtualenv setup is handled by _install_dependencies.sh_)

### Build environment setup ###
Before building NEPI-BOT, you must activate the Python virtual environment
```
$ source ./utilities/venv/bin/activate
```

>Note: If using the unified _nepi_engine_build_complete.sh_ build script, the virtualenv will be activated automatically as part of the build process.

>Note: Once you source virtual environment in the terminal, follow the rest of the NEPI-BOT build and install steps in the same terminal.

### NEPI-BOT Creation
1) Build/install NEPI-BOT binary software using the “create_bot_istallation.py” python script in the utilities folder. You can provide a 10 digit NEPI Unique ID (NUID) as an argument when launching the script, or you can provide the default "UNSET" value and update it later at a convenient time using a provided utility script. This NUID and the public key created during the installation will be used later to register your NEPI-EDGE device in the NEPI-REMOTE application running on a remote server or Cloud.

```
$ cd ./utilities
$ python ./create_bot_installation.py -n <10-digit number or 'UNSET'>
```

***EXAMPLE:***

```
$ python ./create_bot_installation # NUID will be UNSET
```
or
```
$ python ./create_bot_installation.py -n 7777777777
```

> Note: When choosing an NUID, we suggest using a random number generator to create it. The output distribution will be placed in a dist folder at the root of this repository in a folder named “dist/nuid_##########/installDate/”. The subdirectories therein are differentiated by NUID and a build timestamp, so you may build for multiple NUIDs and multiple versions of the repo with the same NUID without overwriting prior build artifacts in the dist folder.

> Note: To install NEPI-BOT (either binary form or script form) to a permanent filesystem location, you can provide the 
  --install_binary <path/to/binary/install/folder> OR
  --install_script <path/to/script/install/folder>

  ***EXAMPLE:***
  ```
  $ python ./create_bot_installation.py -n 7777777777 --install_binary /opt/nepi/nepi_link/nepi-bot
  ```

> Note: **/opt/nepi/nepi_link/nepi-bot is the strongly preferred install location for best automatic interoperability with the rest of the NEPI s/w.**

#### Setting or Updating the NUID ####
A valid 10-digit NUID can be set or updated after NEPI-BOT is built and installed using the _change_identity.py_ script that is installed along with the NEPI-BOT instance:

***EXAMPLE:***
```
$ cd /opt/nepi/nepi_link/nepi-bot/devinfo
$ ./change_identity.py -n 1234567890 
```
This will overwrite the NUID (including special UNSET value) and update SSH keys.
> **Note: Running this script will invalidate any previous NEPI-CONNECT registration for the current NUID**

### NEPI-Bot Registration ###
Before the NEPI-BOT instance can communicate with the NEPI-CONNECT instance, you must register the device through the NEPI-CONNECT instance. You may also need to modify the top-level NEPI-BOT configuration file to specify the correct network address for the NEPI-CONNECT instance.

#### Obtaining the public key ####
In order to register a new NEPI-CONNECT device through the NEPI-CONNECT instance, you must provide the device public key at registration time. You can display the value of this key with
```
$ cd ./dist/nuid_<NUID>/<installDate>/ssh_keys
$ cat id_rsa_<NUID>.pub
```

>Note: Replace values in brackets < > with your specific installation values; Make sure to access the .pub file.

***EXAMPLE:***

```
$ cd /home/nepi/nepi-bot/dist/nuid_7777777777/2022_06_21_09_39_04/ssh_keys
$ cat id_rsa_7777777777.pub
```

>Note: Copy the ssh key displayed. This key will be used in the next section to register your NEPI-BOT enabled device in a NEPI-REMOTE Portal installation.

#### Setting up for a custom NEPI-CONNECT instance
If using a NEPI-CONNECT instance other than the Numurus https://nepi.io portal, you must edit the top-level NEPI-BOT configuration file. If you've installed to the recommended /opt/nepi/nepi_link/nepi-bot folder, this file is at
/opt/nepi/nepi_link/nepi-bot/cfg/bot/config.json

Edit the "host" field in the _hb_ip_ (and _lb_ip_ if configuring for LB over IP) with your custom instance's IPv4 address or internet-resolvable hostname and save the edited file.

#### Registering your NEPI-BOT Enabled Device in a NEPI-CONNECT instance

1) Open a web browser and navigate to https://nepi.io (or a custom NEPI-REMOTE installation), and enter your NEPI-REMOTE account credentials to login.

2) On the Fleet Portal tab, select the **“Add Device”** button to the right under the map view.

3) Paste the NUID for your device, provide a name (Alias) for your device, and paste the public key you copied from your NEPI-BOT enabled edge device.

4) Select Add Device button at the bottom


>Note: All NEPI communications are device initiated, so no interaction between NEPI-BOT and NEPI-CONNECT occurs until you provide NEPI-BOT data to upload and run the NEPI-BOT application on the edge device as explained in the next section.

## Next Steps
Now you are ready to populate LB and HB data, launch NEPI-BOT, view transmitted data in NEPI-CONNECT, etc. If you have not yet done so, take a look at the [NEPI Remote System Interfacing API](https://numurus.com/wp-content/uploads/API-Manual-NEPI-Engine-Remote-System-Interfacing.pdf)

### Standard usage:
- Use the ROS API of the _nepi_link_ros_bridge_ ROS node from the repository of the same name to control NEPI-BOT programmatically or interactively
- Use the NEPI RUI's "Connect" app to control NEPI-BOT interactively

### Custom usage:
- Clone the nepi_edge_sdk_link repository and follow the instructions included in the README.md file to use nepi_edge_sdk_link libraries/modules in your own custom apps to control your NEPI-BOT instance.
- Use the NEPI-BOT file-based interface to setup NEPI-CONNECT sessions and launch NEPI-BOT directly at the device command line to service those sessions.
