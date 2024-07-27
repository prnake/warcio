from warcio.bufferedreaders import DecompressingBufferedReader
from contextlib import closing
from io import BytesIO

def chunk_html_checker(chunk, chunk_size):
    # return False
    if chunk_size == 1:
        return False
    if chunk_size > 160: # 10M or infinite loop
        return False
    return True

with open("data/cake.br", "rb") as rb:
    with closing(DecompressingBufferedReader(rb, decomp_type='br', extend_callback=chunk_html_checker)) as x:
        print(x.read(10))
with open("data/cake.gzip", "rb") as rb:
    with closing(DecompressingBufferedReader(rb, decomp_type='gzip', extend_callback=chunk_html_checker)) as x:
        print(x.read(10))