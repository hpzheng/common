from pdb_data import modules, config
import os, sys
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-m', '--mode', default="local", help="run mode (local or pbs)")
parser.add_option('-n', '--dry-run', dest="dry_run", action="store_true", default=False, help="dry-run: search and initialize  modules and resolve dependecies without actually running them")

(options, args) = parser.parse_args()

ms = []
os.path.walk(config.repo_dir, modules.extract, ms)
if len(args) > 0:
  m = [a for a in ms if a.name in args]
  # Add dependencies
  m.extend(modules.get_all_dependencies(m))
  ms = list(set(m))

modules_to_run = modules.sort_execution_order(ms)

for module, dependencies in modules_to_run:
    try:
        if not options.dry_run:
            module.run(dependencies, options.mode)
    except Exception as err:
        module.logger.error(err)
        raise
