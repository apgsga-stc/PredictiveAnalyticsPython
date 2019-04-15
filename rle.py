### Run Length Encoding
# Python-Implementation der einfachen Komprimierung
# Keine rekursive Komprimierung von Sub-Elementen!


from itertools import groupby, chain, repeat
from typing import Iterable

def encode(seq: Iterable) -> list:
    """Encodes sequence into a list of runs (RLE)"""
    return [(element, len(tuple(grp))) for element, grp in groupby(seq)]


def decode(rle: list) -> Iterable:
    """Decodes RLE-encoded sequence, returns iterator"""
    return chain.from_iterable(repeat(element, cnt) for element, cnt in rle)


def decode_str(rle: list, sep='') -> str:
    """Decodes RLE-encoded sequence into string"""
    return sep.join(map(str, decode(rle)))


###############################################################################
# TESTING CODE
###############################################################################

if __name__ == "__main__":
    from pa_log import time_log
    from pa_util import obj_size

    src = [(1,2), (1,2), 'a', 'a', 3, 3, 5, 5, 5]
    print(f'{src} ==> {encode(src)}')
    print(list(decode(encode(src))) == src)

    @time_log('encoding')
    def many_encode(n_runs, inp):
        for i in range(n_runs):
            encode(inp)

    @time_log('decoding')
    def many_decode(n_runs, inp):
        dummy_enc = encode(inp)
        for i in range(n_runs):
            list(decode(dummy_enc))

    many_encode(10000, 'aaaabbcddeee')
    many_decode(10000, 'aaaabbcddeee')

    src = chain(repeat(1, 1000), repeat('a', 1000))
    print(f'Size of "chain(repeat(1, 1000), repeat(\'a\', 1000))": {obj_size(list(src))} ==> {obj_size(encode(src))}')