#!/usr/bin/env python

"""Create script and binary distributables for nepi-bot

See the help menu (-h or --help) for usage.

Prior to running this script, you must ensure that the dependencies called out in dev-requirements.txt are met.
The easiest and safest way to accomplish that is to set up a virtual environment and install dependencies there.
From the root of this repository:
    $ python3 -m virtualenv dev_venv
    $ source ./dev_venv/bin/activate
    $ pip3 install -r dev-requirements.txt
will create the virtual environment and prepare your system to run this script. The dev_venv contents SHOULD NOT
be checked into the repository -- these are particular to your system. After you've created this virtual env.,
only step 2 ($ source ./dev_venv/bin/activate) is necessary in future sessions.

You must also add the nepi_edge_sw_mgr.py "package" in such a way that it can be found. The best way to do that
is to add a .pth file in the site-packages file of your virtualenv:

    $ cd $(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
    $ echo /abs/path/to/nepi_edge_sw_mgr_submodule > nepi_edge_sw_mgr.pth

This script should be run from the utilities directory. The output will be placed in a dist folder at the root of
this repository. The subdirectories therein are differentiated by NUID and a build timestamp, so you may build
for multiple NUIDs and multiple versions of the repo with the same NUID without overwriting prior build artifacts
in the dist folder

This script requires the --nuid argument, which specifies the NUID for this instance. Prior to running the instance
in script or executable form, it must be provisioned on the server side using matching NUID.

If no (private) SSH key is provided via the --ssh-priv-key argument, a public/private key-pair will be generated.
The private key will be included in the distributables, while the public key should be copied to the server as part
of the provisioning step. When prompted for the SSH key phrase during key generation, leave the key phrase blank...
A future iteration of this script will do that for you automatically.
"""

import os
import shutil
from datetime import datetime
import subprocess
import argparse
import sys

parser = argparse.ArgumentParser(description='Create script and binary distributables for the current nepi-bot repo')
parser.add_argument('-n','--nuid', required=True, nargs=1, help='Provide the NUID for the output distributables')
parser.add_argument('-s', '--ssh_priv_key', required=False, nargs=1, help='File with the private key to be deployed to this instance. Keys will be generated if this arg is not present')
parser.add_argument('-c', '--config_file', required=False, nargs=1, help='Config file (config.json) for this instance. If not supplied, the config file checked into the repo will be used')
parser.add_argument('-d', '--database_file', required=False, nargs=1, help='Database file (nepibot.db) for this instance. If not supplied, nepi-bot will construct a new database the first time it is run')
parser.add_argument('-u', '--update_from', required=False, nargs=1, help='Convenience to set the --ssh_priv_key, --config_file, and --database_file args from an existing nepi-bot installation folder')
parser.add_argument('--install_binary', required=False, nargs=1, help='Convenience to install resulting binary distribution to a local folder. The old contents of that folder (if any) will be deleted')
parser.add_argument('--install_script', required=False, nargs=1, help='Convenience to install resulting script distribution to a local folder. The old contents of that folder (if any) will be deleted')

args = parser.parse_args()
#print(args)

# If using --update_from convenience argument, set the other args from that folder path
if args.update_from is not None:
    if args.ssh_priv_key is not None or args.config_file is not None or args.database_file is not None:
        print('Cannot use --update_from argument with any other arguments')
        sys.exit(1)

    # Do a safety check -- if the old NUID doesn't match the new NUID, probably don't want to proceed
    old_install_dir = args.update_from[0]
    with open(old_install_dir + '/devinfo/devnuid.txt', 'r') as f:
        old_nuid = f.read()
    if old_nuid != args.nuid[0]:
        response = input(f'Warning: existing NUID ({old_nuid}) does not match new NUID ({args.nuid[0]}) -- this is probably not what you want to do. Press \'y\' to continue anyway: ')
        if response != 'y':
            sys.exit(1)

    args.ssh_priv_key = [old_install_dir + '/devinfo/devsshkeys.txt']
    args.config_file = [old_install_dir + '/cfg/bot/config.json']
    args.database_file = [old_install_dir + '/db/nepibot.db']

# First, create the distributables folder structure
now_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
build_folder = os.getcwd() + '/../dist/nuid_' + args.nuid[0] + '/' + now_str
os.makedirs(build_folder)

# Add some version info
version_cmd = subprocess.Popen(['git', '--no-pager', 'describe', '--tags'], stdout=subprocess.PIPE, encoding='utf-8')
version_cmd_out = version_cmd.communicate()[0]
status_cmd = subprocess.Popen(['git', '--no-pager', 'status', '--porcelain'], stdout=subprocess.PIPE, encoding='utf-8')
status_cmd_out = status_cmd.communicate()[0]
diff_cmd = subprocess.Popen(['git', '--no-pager', 'diff'], stdout=subprocess.PIPE, encoding='utf-8')
diff_cmd_out = diff_cmd.communicate()[0]
with open(build_folder + '/version.txt', 'w') as f:
    f.write('Version:')
    f.write('\n\t' + version_cmd_out)
    f.write('\n'+'-'*80)
    f.write('Status:')
    for line in status_cmd_out.splitlines():
        f.write('\n\t' + line)
    f.write('\n'+'-'*80)
    f.write('Diff:')
    for line in diff_cmd_out.splitlines():
        f.write('\n\t' + line)

script_build_folder = build_folder + '/nepi-bot'
binary_build_folder = build_folder + '/nepi-bot-binary'

# Now create the basic subdirectory structure for the script-form distributable
# Start with the portions that are identical between the script distributable and binary distributable
shutil.copytree('../cfg', script_build_folder + '/cfg') # Verbatim copy
if args.config_file is not None: # Overwrite the config.json file if argument supplied
    shutil.copyfile(args.config_file[0], script_build_folder + '/cfg/bot/config.json')

os.mkdir(script_build_folder + '/db')
if args.database_file is not None: # Overwrite the nepibot.db file if argument is supplied
    shutil.copyfile(args.database_file[0], script_build_folder + '/db/nepibot.db')

lb_folder = script_build_folder + '/lb'
os.mkdir(lb_folder)
os.mkdir(lb_folder + '/cfg')
os.mkdir(lb_folder + '/data')
os.mkdir(lb_folder + '/do-msg')
os.mkdir(lb_folder + '/dt-msg')

hb_folder = script_build_folder + '/hb'
os.mkdir(hb_folder)
os.makedirs(hb_folder + '/do')
os.makedirs(hb_folder + '/dt')

os.mkdir(script_build_folder + '/log')

# Device info should be populated here
devinfo_folder = script_build_folder + '/devinfo'
os.mkdir(devinfo_folder)
with open(devinfo_folder + '/devnuid.txt', 'w') as f:
    f.write(args.nuid[0])
shutil.copy2('./change_identity.py', devinfo_folder + '/change_identity.py')

ssh_keys_folder = build_folder + '/ssh_keys'
os.mkdir(ssh_keys_folder)
ssh_key_filename = ssh_keys_folder + '/id_rsa_' + args.nuid[0]
public_ssh_key_filename = ssh_key_filename + '.pub'
if args.ssh_priv_key is None:
    # TODO: Ensure key adheres to our security standard
    subprocess.call(['ssh-keygen', '-f' + ssh_key_filename])
else:
    shutil.copyfile(args.ssh_priv_key[0], ssh_key_filename)
    shutil.copyfile(os.path.dirname(args.ssh_priv_key[0]) + '/id_rsa_' + args.nuid[0] + '.pub', public_ssh_key_filename)
    with open(ssh_keys_folder + '/README.txt', 'w') as f:
        f.write('SSH private key copied from ' + args.ssh_priv_key[0])
shutil.copyfile(ssh_key_filename, devinfo_folder + '/' + 'devsshkeys.txt')
shutil.copyfile(public_ssh_key_filename, devinfo_folder + '/' + os.path.basename(public_ssh_key_filename))

# Now copy all of the above to the binary distributable as it is identical in both cases
shutil.copytree(script_build_folder, binary_build_folder)

# Populate the src folder contents in a separate tmp directory -- we'll copy this verbatim to the script-based distributable
# and process it into a binary for the binary distributable
tmp_folder = build_folder + '/tmp'
tmp_src_folder = tmp_folder + '/src'
if (os.path.exists(tmp_src_folder)):
    shutil.rmtree(tmp_src_folder)
os.makedirs(tmp_src_folder)

shutil.copytree('../src/bot', tmp_src_folder + '/bot') # Verbatim copy
cwd=os.getcwd()
# Need to generate the protobuf python files in the source folder
os.chdir('../nepi-protobuf')
subprocess.call(['protoc', '--proto_path=.', '--python_out='+tmp_src_folder+'/bot', 'nepi_messaging-all.proto'])
subprocess.call(['protoc', '--proto_path=.', '--python_out='+tmp_src_folder+'/bot', 'timestamp.proto'])
os.chdir(cwd)

# Now copy it to scripts distributable
shutil.copytree(tmp_src_folder, script_build_folder + '/src')

# Prepare a runtime virtualenv for the scripts distributable -- the pip initialization of the venv must happen at the location
# of the final installation (container or otherwise) and based on a pip that installs for the proper architecture, so we can't do
# it here
shutil.copyfile('../runtime-requirements.txt', script_build_folder + '/requirements.txt')
os.mkdir(script_build_folder + '/venv')

# Now process src folder into a binary for binary distributable

os.chdir(tmp_src_folder + '/bot')
subprocess.call(['pyinstaller', 'botmain.py'])
os.chdir(cwd)
shutil.copytree(tmp_src_folder + '/bot/dist/botmain', binary_build_folder + '/bin/botmain')
shutil.rmtree(tmp_folder)

print('\n\nFinished building distributables... outputs can be found in ' + build_folder)

if args.install_binary is not None:
    install_dir = args.install_binary[0]
    print('\n\nInstalling binary distributable to ' + install_dir)

    response = 'y'
    if os.path.isdir(install_dir):
        response = input(f'Warning: {install_dir} already exists... enter \'y\' to overwrite, any other key to cancel: ')
        if response == 'y':
            shutil.rmtree(install_dir)
    if response == 'y':
        shutil.copytree(binary_build_folder, install_dir)
        print('... binary installation complete')

if args.install_script is not None:
    install_dir = args.install_script[0]
    print('\n\nInstalling script distributable to ' + install_dir)

    response = 'y'
    if os.path.isdir(install_dir):
        response = input(f'Warning: {install_dir} already exists... enter \'y\' to overwrite, any other key to cancel: ')
        if response == 'y':
            shutil.rmtree(install_dir)
    if response == 'y':
        shutil.copytree(script_build_folder, install_dir)
        print('... script installation complete')
