import os

# Get path of the scripts relative to this config file
scripts_dir = os.path.dirname(os.path.abspath(__file__))
# Repository dir should be two dirs above
repo_dir = os.path.abspath(os.path.join(scripts_dir, '..', '..'))
# Read repository configuration
with open(os.path.join(repo_dir, 'common', 'config', 'paths')) as f:
        paths = dict(((l[0], l[1]) for l in (ln.split() for ln in f.readlines())))
        pdb_repository_dir = os.path.abspath(paths['pdb-repository'])
