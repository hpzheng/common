from pdb_data import modules, config
import os, sys

ms = []
os.path.walk(config.repo_dir, modules.extract, ms)
modules_to_run = modules.sort_execution_order(ms)

if len(sys.argv) > 1:
    mode = sys.argv[1]
else:
    mode = 'local'

for module, dependencies in modules_to_run:
    try:
        module.run(dependencies, mode)
    except Exception as err:
        module.logger.error(err)
        raise
