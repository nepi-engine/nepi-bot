#!/usr/bin/env python

"""Create script and binary distributables for nepi-bot

See the help menu (-h or --help) for usage.

Prior to running this script, you must ensure that the dependencies called out in dev-requirements.txt are met.
The easiest and safest way to accomplish that is to set up a virtual environment and install dependencies there.
From the root of this repository:
    $ python -m virtualenv dev_venv
    $ source ./dev_venv/bin/activate
    $ pip install -m dev-requirements.txt
will create the virtual environment and prepare your system to run this script. The dev_venv contents SHOULD NOT
be checked into the repository -- these are particular to your system.

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

parser = argparse.ArgumentParser(description='Create script and binary distributables for the current nepi-bot repo')
parser.add_argument('-n','--nuid', required=True, nargs=1, help='Provide the NUID for the output distributables')
parser.add_argument('-s', '--ssh_priv_key', required=False, nargs=1, help='File with the private key to be deployed to this instance. Keys will be generated if this arg is not present')
parser.add_argument('-c', '--config_file', required=False, nargs=1, help='Config file (config.json) for this instance. If not supplied, the config file checked into the repo will be used')

args = parser.parse_args()
#print(args)

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

lb_folder = script_build_folder + '/lb'
os.mkdir(lb_folder)
os.mkdir(lb_folder + '/cfg')
os.mkdir(lb_folder + '/data')
os.mkdir(lb_folder + '/do-msg')
os.mkdir(lb_folder + '/dt-msg')

hb_folder = script_build_folder + '/hb'
os.mkdir(hb_folder)
os.makedirs(hb_folder + '/clone/do')
os.makedirs(hb_folder + '/clone/dt')

os.mkdir(script_build_folder + '/log')

# Device info should be populated here
devinfo_folder = script_build_folder + '/devinfo'
os.mkdir(devinfo_folder)
with open(devinfo_folder + '/devnuid.txt', 'w') as f:
    f.write(args.nuid[0])

ssh_keys_folder = build_folder + '/ssh_keys'
os.mkdir(ssh_keys_folder)
ssh_key_filename = ssh_keys_folder + '/id_rsa_' + args.nuid[0]
if args.ssh_priv_key is None:
    # TODO: Ensure key adheres to our security standard
    subprocess.call(['ssh-keygen', '-f' + ssh_key_filename])
else:
    shutil.copyfile(args.ssh_priv_key[0], ssh_key_filename)
    with open(ssh_keys_folder + '/README.txt', 'w') as f:
        f.write('SSH private key copied from ' + args.ssh_priv_key[0])
shutil.copyfile(ssh_key_filename, devinfo_folder + '/' + 'devsshkeys.txt')

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

print('\n\nAll done... distributables can be found in ' + build_folder)
