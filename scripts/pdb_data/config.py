import os

# Get path of the scripts relative to this config file
scripts_dir = os.path.dirname(os.path.abspath(__file__))
# Repository dir should be three dirs above
repo_dir = os.path.abspath(os.path.join(scripts_dir, '..', '..', '..'))
config_dir = os.path.join(repo_dir, 'common', 'config')
# Read repository configuration
with open(os.path.join(config_dir, 'paths')) as f:
    paths = dict(((l[0], l[1]) for l in (ln.split() for ln in f.readlines())))

pdb_repository_dir = os.path.abspath(paths['pdb-repository'])

default={}

# Distributed computing configurations
with open(os.path.join(config_dir, 'pbs')) as f:
    pbs = dict(((l[0], l[1]) for l in (ln.split() for ln in f.readlines())))

default['max_cores']  = int(pbs['max-cores'])
default['queue_name'] = pbs['queue-name']
default['wall_time']  = int(pbs['default-time'])
default['timeout']    = int(pbs['timeout'])
