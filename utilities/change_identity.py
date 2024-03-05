#!/usr/bin/env python3
#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#

"""Change the NUID and SSH keys for a given bot installation

Typically this script is run at the factory to update from a default nepi-bot installation
to a new instance. Note that it overwrites the NUID and SSH keys without prompt, and
the old, private SSH keys cannot be easily recovered. If running this script on
anything but the default bot installation, make sure you know what you are doing.

The script must be run from the devinfo folder of a previously installed folder -- if it doesn't exist in that folder,
likely you are not

See the help menu (-h or --help) for usage.
"""

import sys
import glob
import os
import subprocess
import argparse
import shutil

PRIV_SSH_KEY_FILENAME = './devsshkeys.txt'

parser = argparse.ArgumentParser(description='Change the NUID and SSH keys for a given bot installation')
parser.add_argument('-n','--nuid', required=True, nargs=1, help='Provide the new NUID')

args = parser.parse_args()

# Make sure the files exist as expected as a safety check
if (os.path.isfile('./devnuid.txt') is False) or (os.path.isfile(PRIV_SSH_KEY_FILENAME) is False):
    print("Current directory is not part of a valid nepi-bot installation... aborting")
    sys.exit(1)

# Overwrite the NUID
with open('./devnuid.txt', 'w') as f:
    f.write(args.nuid[0])

# Delete the old SSH keys
os.remove(PRIV_SSH_KEY_FILENAME)
public_key_filelist = glob.glob('./*.pub', recursive=False)
for f in public_key_filelist:
    os.remove(f)

# Create the new SSH keys
subprocess.call(['ssh-keygen', '-f', './id_rsa', '-N', ''])
shutil.move('./id_rsa', PRIV_SSH_KEY_FILENAME)
public_key_filename = './id_rsa_' + args.nuid[0] + '.pub'
shutil.move('./id_rsa.pub', public_key_filename)

# That's it
print("Identity change complete: New nuid = " + args.nuid[0])
# Print the public key in case the device is being registered right now
print('Here is your new public key:')
with open(public_key_filename, 'r') as f:
    print(f.read())
