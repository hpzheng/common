import config
from glob import iglob
import os

def codes_with_structure_factors():
    for fn in iglob(os.path.join(config.structure_factors_path, '*')):
        code = os.path.basename(fn).split('.')[0][1:5]
        yield code

def codes_current():
    for fn in iglob(os.path.join(config.entries_path, '*')):
        code = os.path.basename(fn).split('.')[0][3:7]
        yield code

if __name__ == '__main__':
    sf = list(codes_with_structure_factors())
    cc = list(codes_current())
    print 'Current entries: ', len(cc)
    print ' ', ', '.join(cc[:5]), '...', ', '.join(cc[-5:])
    print 'With structure factors: ', len(sf)
    print ' ', ', '.join(sf[:5]), '...', ', '.join(sf[-5:])
