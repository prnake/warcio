from io import BytesIO
import zlib
import sys

from warcio.utils import BUFF_SIZE


class ZlibDecompressor:
    """
    An object that allows for streaming decompression of zlib-compressed data.
    """

    def __init__(self, encoding):
        assert encoding in ['gzip', 'deflate', 'deflate_alt']
        if encoding == "gzip":
            self._decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        elif encoding == "deflate":
            self._decompressor = zlib.decompressobj()
        elif encoding == "deflate_alt":
            self._decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
        self._buffer_size = 65536
        self.remaining_data = b''

    def decompress(self, data, extend_callback=None):
        """
        Decompress part of a complete zlib-compressed string.

        :param data: A bytestring containing zlib-compressed data.
        :param extend_callback: A callback function to handle each decompressed chunk.
        :returns: A bytestring containing the decompressed data.
        """
        chunks = []
        # self.remaining_data = self.remaining_data + data
        self.remaining_data = self.remaining_data + data

        while self.remaining_data:
            decompressed_chunk = self._decompressor.decompress(self.remaining_data, self._buffer_size)
            chunks.append(decompressed_chunk)
            # print(decompressed_chunk, len(chunks))
            if extend_callback:
                flag = extend_callback(decompressed_chunk, len(chunks))
                if not flag:
                    break
            self.remaining_data = self._decompressor.unconsumed_tail
            if len(decompressed_chunk) < self._buffer_size:
                break

        return b''.join(chunks)

    def flush(self):
        """
        Complete the decompression, returning any remaining data to be decompressed.

        :returns: A bytestring containing the remaining decompressed data.
        """
        return self._decompressor.flush()
    
    @property
    def unused_data(self):
        """
        Return any unused data from the decompression process.

        :returns: A bytestring containing the unused data.
        """
        return self._decompressor.unused_data


#=================================================================
def try_brotli_init():
    try:
        import warcio.brotli

        def brotli_decompressor():
            decomp = warcio.brotli.Decompressor()
            decomp.unused_data = None
            return decomp

        BufferedReader.DECOMPRESSORS['br'] = brotli_decompressor
    except ImportError:  #pragma: no cover
        pass


#=================================================================
class BufferedReader(object):
    """
    A wrapping line reader which wraps an existing reader.
    Read operations operate on underlying buffer, which is filled to
    block_size (16384 default)

    If an optional decompress type is specified,
    data is fed through the decompressor when read from the buffer.
    Currently supported decompression: gzip
    If unspecified, default decompression is None

    If decompression is specified, and decompress fails on first try,
    data is assumed to not be compressed and no exception is thrown.

    If a failure occurs after data has been
    partially decompressed, the exception is propagated.

    """

    DECOMPRESSORS = {'gzip': lambda: ZlibDecompressor('gzip'),
                     'deflate': lambda: ZlibDecompressor('deflate'),
                     'deflate_alt': lambda: ZlibDecompressor('deflate_alt'),
                    }

    def __init__(self, stream, block_size=BUFF_SIZE,
                 decomp_type=None,
                 starting_data=None,
                 read_all_members=False,
                 extend_callback=None):
        
        self.stream = stream
        self.block_size = block_size

        self._init_decomp(decomp_type)

        self.buff = None
        self.starting_data = starting_data
        self.num_read = 0
        self.buff_size = 0

        self.read_all_members = read_all_members

        self.extend_callback = extend_callback

    def set_decomp(self, decomp_type):
        self._init_decomp(decomp_type)

    def _init_decomp(self, decomp_type):
        self.num_block_read = 0
        if decomp_type:
            try:
                self.decomp_type = decomp_type
                self.decompressor = self.DECOMPRESSORS[decomp_type.lower()]()
            except KeyError:
                raise Exception('Decompression type not supported: ' +
                                decomp_type)
        else:
            self.decomp_type = None
            self.decompressor = None

    def _fillbuff(self, block_size=None):
        if not self.empty():
            return

        # can't read past next member
        if self.rem_length() > 0:
            return

        block_size = block_size or self.block_size

        if self.starting_data:
            data = self.starting_data
            self.starting_data = None
        else:
            data = self.stream.read(block_size)

        self._process_read(data)

        # if raw data is not empty and decompressor set, but
        # decompressed buff is empty, keep reading --
        # decompressor likely needs more data to decompress
        while data and self.decompressor and not self.decompressor.unused_data and self.empty():
            data = self.stream.read(block_size)
            self._process_read(data)

    def _process_read(self, data):
        # don't process if no raw data read
        if not data:
            self.buff = None
            return

        data = self._decompress(data)
        self.buff_size = len(data)
        self.num_read += self.buff_size
        self.num_block_read += self.buff_size
        self.buff = BytesIO(data)

    def _decompress(self, data):
        if self.decompressor and data:
            try:
                data = self.decompressor.decompress(data, self.extend_callback)
            except Exception as e:
                # if first read attempt, assume non-gzipped stream
                if self.num_block_read == 0:
                    if self.decomp_type == 'deflate':
                        self._init_decomp('deflate_alt')
                        data = self._decompress(data)
                    else:
                        self.decompressor = None
                # otherwise (partly decompressed), something is wrong
                else:
                    sys.stderr.write(str(e) + '\n')
                    return b''
        return data

    def read(self, length=None):
        """
        Fill bytes and read some number of bytes
        (up to length if specified)
        <= length bytes may be read if reached the end of input
        if at buffer boundary, will attempt to read again until
        specified length is read
        """
        all_buffs = []
        while length is None or length > 0:
            self._fillbuff()
            if self.empty():
                break

            buff = self.buff.read(length)
            all_buffs.append(buff)
            if length:
                length -= len(buff)

        return b''.join(all_buffs)



    def readline(self, length=None):
        """
        Fill buffer and read a full line from the buffer
        (up to specified length, if provided)
        If no newline found at end, try filling buffer again in case
        at buffer boundary.
        """
        if length == 0:
            return b''

        self._fillbuff()

        if self.empty():
            return b''

        linebuff = self.buff.readline(length)

        # we may be at a boundary
        while not linebuff.endswith(b'\n'):
            if length:
                length -= len(linebuff)
                if length <= 0:
                    break

            self._fillbuff()

            if self.empty():
                break

            linebuff += self.buff.readline(length)

        return linebuff

    def tell(self):
        return self.num_read

    def empty(self):
        if not self.buff or self.buff.tell() >= self.buff_size:
            # if reading all members, attempt to get next member automatically
            if self.read_all_members:
                self.read_next_member()

            return True

        return False

    def read_next_member(self):
        if not self.decompressor or not self.decompressor.unused_data:
            return False

        self.starting_data = self.decompressor.unused_data
        self._init_decomp(self.decomp_type)
        return True

    def rem_length(self):
        rem = 0
        if self.buff:
            rem = self.buff_size - self.buff.tell()

        if self.decompressor and self.decompressor.unused_data:
            rem += len(self.decompressor.unused_data)
        return rem

    def close(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        self.buff = None

        self.close_decompressor()

    def close_decompressor(self):
        if self.decompressor:
            self.decompressor.flush()
            self.decompressor = None

    @classmethod
    def get_supported_decompressors(cls):
        return cls.DECOMPRESSORS.keys()


#=================================================================
class DecompressingBufferedReader(BufferedReader):
    """
    A BufferedReader which defaults to gzip decompression,
    (unless different type specified)
    """
    def __init__(self, *args, **kwargs):
        if 'decomp_type' not in kwargs:
            kwargs['decomp_type'] = 'gzip'
        super(DecompressingBufferedReader, self).__init__(*args, **kwargs)


#=================================================================
class ChunkedDataException(Exception):
    def __init__(self, msg, data=b''):
        Exception.__init__(self, msg)
        self.data = data


#=================================================================
class ChunkedDataReader(BufferedReader):
    r"""
    A ChunkedDataReader is a DecompressingBufferedReader
    which also supports de-chunking of the data if it happens
    to be http 'chunk-encoded'.

    If at any point the chunked header is not available, the stream is
    assumed to not be chunked and no more dechunking occurs.
    """
    def __init__(self, stream, raise_exceptions=False, **kwargs):
        super(ChunkedDataReader, self).__init__(stream, **kwargs)
        self.all_chunks_read = False
        self.not_chunked = False

        # if False, we'll use best-guess fallback for parse errors
        self.raise_chunked_data_exceptions = raise_exceptions

    def _fillbuff(self, block_size=None):
        if self.not_chunked:
            return super(ChunkedDataReader, self)._fillbuff(block_size)

        # Loop over chunks until there is some data (not empty())
        # In particular, gzipped data may require multiple chunks to
        # return any decompressed result
        while (self.empty() and
               not self.all_chunks_read and
               not self.not_chunked):

            try:
                length_header = self.stream.readline(64)
                self._try_decode(length_header)
            except ChunkedDataException as e:
                if self.raise_chunked_data_exceptions:
                    raise

                # Can't parse the data as chunked.
                # It's possible that non-chunked data is served
                # with a Transfer-Encoding: chunked.
                # Treat this as non-chunk encoded from here on.
                self._process_read(length_header + e.data)
                self.not_chunked = True

                # parse as block as non-chunked
                return super(ChunkedDataReader, self)._fillbuff(block_size)

    def _try_decode(self, length_header):
        # decode length header
        try:
            # ensure line ends with \r\n
            assert(length_header[-2:] == b'\r\n')
            chunk_size = length_header[:-2].split(b';')[0]
            chunk_size = int(chunk_size, 16)
            # sanity check chunk size
            assert(chunk_size <= 2**31)
        except (ValueError, AssertionError):
            raise ChunkedDataException(b"Couldn't decode length header " +
                                       length_header)

        if not chunk_size:
            # chunk_size 0 indicates end of file. read final bytes to compute digest.
            final_data = self.stream.read(2)
            try:
                assert(final_data == b'\r\n')
            except AssertionError:
                raise ChunkedDataException(b"Incorrect \r\n after length header of 0")
            self.all_chunks_read = True
            self._process_read(b'')
            return

        data_len = 0
        data = b''

        # read chunk
        while data_len < chunk_size:
            new_data = self.stream.read(chunk_size - data_len)

            # if we unexpectedly run out of data,
            # either raise an exception or just stop reading,
            # assuming file was cut off
            if not new_data:
                if self.raise_chunked_data_exceptions:
                    msg = 'Ran out of data before end of chunk'
                    raise ChunkedDataException(msg, data)
                else:
                    chunk_size = data_len
                    self.all_chunks_read = True

            data += new_data
            data_len = len(data)

        # if we successfully read a block without running out,
        # it should end in \r\n
        if not self.all_chunks_read:
            clrf = self.stream.read(2)
            if clrf != b'\r\n':
                raise ChunkedDataException(b"Chunk terminator not found.",
                                           data)

        # hand to base class for further processing
        self._process_read(data)


#=================================================================
try_brotli_init()

