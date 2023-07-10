# Setting Up NEPI-BOT on an Edge Device

NOTE: Only successfully tested on Ubuntu 18.04 and 20.04

## Clone, Build and Configure NEPI-BOT on Edge Device
### Getting Source Code on the Target
There are two approaches for deploying source code to the target: Cloning this Git repo directly to the target or cloning this Git repo to a development host, then pushing to the target via Rsync script. Typically, direct cloning is best for Desktop installs while rsync is best for installation on embedded target hardware. Select whichever option makes the most sense for your system.
#### Cloning
Clone the NEPI-BOT repo on your Linux target device. Make sure to use the –recursive git option as shown below. NOTE: An SSH Key has be set up with your device and Bitbucket account.

```

git clone git@bitbucket.org:numurus/nepi-bot.git --recursive

```
#### Pushing via Rsync
From development host, follow previous clone step. Then run the top-level rsync script.

```
cd nepi-bot && ./rsync_src_package_to_target.sh
```
You can set the NEPI_SSH_KEY and NEPI_TARGET_IP environment variables prior to running the script as necessary. The source code will be pushed to 
/home/nepi/nepi-bot
on the target device.

### Installing Target Dependencies

1. Ensure that “socat” software is installed on your target device

```

sudo apt-get install socat

```

1. Ensure that “protobuf-compiler” and “cmake” are installed

```

sudo apt install software-properties-common -y

sudo apt install protobuf-compiler

sudo apt install cmake

```

1. Ensure that “python3.6+” ,“pip3”, and “virtualenv” software is installed on your device

```
python3 --version

sudo apt install python3-pip

pip3 install virtualenv

```
### Virtual Environment Setup

Setup a virtual python environment and install required python modules using the provide install utility. Open a terminal in your “nepi-bot” software folder you installed using git and run.

```

cd /opt/nepi/nepi-bot/utilities

python3 -m virtualenv venv

source ./venv/bin/activate

cd ..

pip3 install -r dev-requirements.txt

```

>Note: Once you start the virtual environment in the terminal, follow the rest of the NEPI-BOT installation steps in the same terminal.

### NEPI-BOT Creation
1) Build/install NEPI-BOT binary software using the “create_bot_istallation.py” python script in the utilities folder you should be in already. You will need to provide a 10 digit NEPI Unique ID (NUID) as an argument when launching the script. This NUID and the public key created during the installation will be used later to register your NEPI-EDGE device in the NEPI-REMOTE application running on a remote server or Cloud.

```
cd /home/nepi/nepi-bot/utilities

python3 ./create_bot_installation.py -n ##########

```

***EXAMPLE:***

```

python3 ./create_bot_installation.py -n 7777777777

```

> Note: Replace ########## with a 10 digit ID. Suggest using a random number generator to create it. The output distribution will be placed in a dist folder at the root of this repository in a folder named “dist/nuid_##########/installDate/”. The subdirectories therein are differentiated by NUID and a build timestamp, so you may build for multiple NUIDs and multiple versions of the repo with the same NUID without overwriting prior build artifacts in the dist folder.

> Note: To install nepi-bot (either binary form or script form) to a permanent filesystem location, you can provide the 
  --install_binary <path/to/binary/install/folder> OR
  --install_script <path/to/script/install/folder>

  ***EXAMPLE:***
  ```
  python3 ./create_bot_installation.py -n 7777777777 --install_binary /opt/nepi/nepi-bot
  ```



2) Access the public key created during your NEPI-BOT installation.

```

cat /home/nepi/nepi-bot/dist/nuid_<##########>/<installDate>/ssh_keys/id_rsa_<##########>.pub

cat id_rsa_"##########".pub

```

>Note: Replace values in brackets < > with your specific installation values; Make sure to access the .pub file.



***EXAMPLE:***

```

cd /home/nepi/nepi-bot/dist/nuid_7777777777/2022_06_21_09_39_04/ssh_keys

cat id_rsa_7777777777.pub

```

>Note: Copy the ssh key displayed. This key will be used in the next section to register your NEPI-BOT enabled device in a NEPI-REMOTE Portal installation.__



## Step B) Register Your NEPI-BOT Enabled Device in a NEPI-REMOTE Portal



1) Open a web browser and navigate to https://nepi.io/ (or a custom NEPI-REMOTE installation), and enter your NEPI-REMOTE account credentials to login.

2) On the Fleet Portal tab, select the **“Add Device”** button to the right under the map view.

3) Paste the NUID for your device, provide a name (Alias) for your device, and paste the public key you copied from your NEPI-BOT enabled edge device.

4) Select Add Device button at the bottom



>Note: All NEPI communications are device initiated, so you won’t see any data until you provide NEPI-BOT data to upload and run the NEPI-BOT application on the edge device as explained in the next section.__

## Next Steps
Clone the NEPI-EDGE-SDK repository and follow the instructions  included in the README.md file to use NEPI-EDGE-SDK in conjunction with your NEPI-BOT.
