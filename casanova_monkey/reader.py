# import csvmonkey
# from io import BytesIO, BufferedReader
# from casanova.reader import make_casanova_reader
# from casanova.exceptions import InvalidFileError

# binary_error = InvalidFileError('casanova_monkey.reader: expecting file in binary mode (e.g. "rb") or BytesIO.')


# def wrapper_csv_reader(f, *args, no_headers=False, **kwargs):
#     if isinstance(f, BufferedReader):
#         if 'b' not in f.mode:
#             raise binary_error
#     elif not isinstance(f, BytesIO):
#         raise binary_error

#     reader = csvmonkey.from_file(f, header=not no_headers)
#     print(reader.get_header())
#     return reader


class CasanovaMonkeyReader(object):
    pass
