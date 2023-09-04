# -*-Mode:python-*-
A = Analysis([' test.py '], pathex=[' MyApp '], hiddenimports=[], hookspath=None, runtime_hooks=None)
a.binaries = A.binaries + [(' libclntsh.so ', '/opt/instantclient_11_2/libclntsh.so.11.1 ', ' BINARY ')]
a.binaries = A.binaries + [(' libnnz11.so ', '/opt/instantclient_11_2/libnnz11.so ', ' BINARY ')]
a.binaries = A.binaries + [(' libocci.so ', '/opt/instantclient_11_2/libocci.so.11.1 ', ' BINARY ')]
a.binaries = A.binaries + [(' libociicus.so ', '/opt/instantclient_11_2/libociicus.so ', ' BINARY ')]
pyz = Pyz(a.pure)
exe = EXE(PYZ, a.scripts, A.binaries, A.zipfiles, A.datas, Name=' Test ', debug=False, strip=None, upx=True,
          console=True)
