from pdb_data import modules, config
import os, sys
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-m', '--mode', default="local", help="run mode (local or pbs)")
parser.add_option('-n', '--dry-run', dest="dry_run", action="store_true", default=False, help="DRY RUN: search and initialize  modules and resolve dependecies without actually running them")
parser.add_option('-D', '--no-dependencies', dest='dependencies', action="store_false", default=True, help="Disables adding dependencies when refreshing specified module")

(options, args) = parser.parse_args()

ms = []
os.path.walk(config.repo_dir, modules.extract, ms)
if len(args) > 0:
  m = [a for a in ms if a.name in args]
  if options.dependencies:# Add dependencies
	 m.extend(modules.get_all_dependencies(m))
  ms = list(set(m))

modules_to_run = modules.sort_execution_order(ms, options.dependencies)

for module, dependencies in modules_to_run:
    try:
        if not options.dry_run:
            module.run(dependencies, options.mode)
        else:
            module.logger.info('DRY RUN: Not executing')
    except Exception as err:
        module.logger.error(err)
        raise
