import config
from glob import iglob
import os

structure_factors_path = os.path.join(config.pdb_repository_dir,
                                      'data', 
                                      'structures',
                                      'all',
                                      'structure_factors'
                                      )
def codes_with_structure_factors():
    for fn in iglob(os.path.join(structure_factors_path, '*')):
        code = os.path.basename(fn).split('.')[0][1:5]
        yield code

if __name__ == '__main__':
    sf = list(codes_with_structure_factors())
    print 'With structure factors: ', len(sf)
    print sf[0:5], '...'
